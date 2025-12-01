use chrono::{DateTime, Local, Utc};
use crc_fast::{checksum_file, CrcAlgorithm::Crc64Nvme};
use extract_text::*;
use helper_lib::{
	self,
	watch_for_quit,
	where_sql,
	datetime::naivedate_to_local,
	paths::path_to_agnostic_relative,
	sql::{CompOp, dbfmt, dbfmt_comp, dbfmt_t, query_single_row_to_tuple, query_to_i64, query_to_tuples}
};
use log::*;
use regex::Regex;
use rusqlite::{Connection};
use simplelog::*;
use std::{
	cmp::Reverse, collections::HashMap, error::Error, fs, path::PathBuf, sync::{Arc, atomic::{AtomicBool, Ordering}}, thread::{self, JoinHandle}
};
use walkdir::WalkDir;
//convert std::time::SystemTime to time::Date
pub mod config;
use config::FilesSet;
pub mod sqlstatements;
use sqlstatements::sql_initialise;

// const STANDARD_FILE_EXCLUSION_PATTERN: &str = r"(?i)([\\/]\.)|([\\/]~)|([\\/]\$)|(__pycache__)|(\.pyc$)|([\\/]build[\\/])|([\\/]target[\\/](debug|release)[\\/])|([\\/]Staging[\\/])|([\\/]node_modules[\\/])|([\\/]edcache[\\/])";

// fn setup_files_sets(files_sets: &mut Vec<FilesSet>) {
// 	let standard_file_exclusion: Regex = Regex::new(STANDARD_FILE_EXCLUSION_PATTERN).unwrap();

// 	if cfg!(target_os = "windows") {
// 		files_sets.push(
// 			FilesSet {
// 				name: String::from("test set"),
// 				local_root_path: PathBuf::from(r"C:\Users\hrag\Sync\Programming\python\FileSearcher\test"),
// 				include_subdirs: true,
// 				include_regex: None,
// 				exclude_regex: Some(standard_file_exclusion.clone()),
// 				dt_from: None,
// 				dt_to: None,
// 				db_dir: PathBuf::from(r"C:\Users\hrag\dbs\FileSearcherDeamon5\test_set"),
// 			}
// 		);
// 		files_sets.push(
// 			FilesSet {
// 				name: String::from("dump"),
// 				local_root_path: PathBuf::from(r"C:\Users\hrag\DUMP"),
// 				include_subdirs: false,
// 				include_regex: None,
// 				exclude_regex: Some(standard_file_exclusion.clone()),
// 				dt_from: None,
// 				dt_to: None,
// 				db_dir: PathBuf::from(r"C:\Users\hrag\dbs\FileSearcherDeamon5\dump"),
// 			}
// 		);
// 	} else if cfg!(target_os = "linux") {
// 		files_sets.push(
// 			FilesSet {
// 				name: String::from("test set"),
// 				local_root_path: PathBuf::from("/home/ray/MEGA/Rays/Programming/python/file/test_text_extract"),
// 				include_subdirs: true,
// 				include_regex: Some(Regex::new(r"(?i).*\.py$").unwrap()),
// 				exclude_regex: Some(standard_file_exclusion.clone()),
// 				dt_from: None,
// 				dt_to: None,
// 				db_dir: PathBuf::from("/home/ray/FileSearcherDeamon5/test_set"),
// 			}
// 		);
// 	} else {
// 		panic!("Unsupported OS");
// 	}
// }

fn initalise_database(files_set: &FilesSet) -> Result<(), Box<dyn Error>> {
	//create directory
	fs::create_dir_all(&files_set.db_dir).unwrap();

	let mut db_path_main = files_set.db_dir.clone();
	db_path_main.push(sqlstatements::MAIN_DB);
	let mut db_path_metadata = files_set.db_dir.clone();
	db_path_metadata.push(sqlstatements::METADATA_DB);
	let mut db_path_contents = files_set.db_dir.clone();
	db_path_contents.push(sqlstatements::CONTENT_DB);

	//check version (if any)
	let cur_db_ver = query_to_i64(&db_path_main, "SELECT setting_value FROM settings WHERE setting_name = 'db_ver';").unwrap_or(Some(0)).unwrap_or(0);

	//delete if different version, otherwise ignore
	if cur_db_ver as i32 != sqlstatements::DB_VER {
		info!("Current db version: {}, need to upgrade to version {}", cur_db_ver, sqlstatements::DB_VER);
		
		//delete the existing db files
		if db_path_main.exists() {
			fs::remove_file(&db_path_main)?;
		}
		if db_path_metadata.exists() {
			fs::remove_file(&db_path_metadata)?;
		}
		if db_path_contents.exists() {
			fs::remove_file(&db_path_contents)?;
		}

		//run initalise scripts
		{
			let conn = Connection::open(&db_path_main)?;
			let sqls = sql_initialise::main_db();
			for sql in sqls {
				let _changes = conn.execute(&sql, [])?;
			}
			let sqls = sql_initialise::insert_settings(&files_set.local_root_path.to_string_lossy());
			for sql in sqls {
				let _changes = conn.execute(&sql, [])?;
			}
		}
		{
			let conn = Connection::open(&db_path_metadata)?;
			let sqls = sql_initialise::metadata_db();
			for sql in sqls {
				let _changes = conn.execute(&sql, [])?;
			}
		}
		{
			let conn = Connection::open(&db_path_contents)?;
			let sqls = sql_initialise::content_db();
			for sql in sqls {
				let _changes = conn.execute(&sql, [])?;
			}
		}
	}

	Ok(())

}

fn main() {
	let logger_config = ConfigBuilder::new()
		.set_time_offset_to_local().expect("Failed to get local time offset")
		.set_time_format_custom(format_description!("[hour]:[minute]:[second].[subsecond digits:3]"))
		.build();
	CombinedLogger::init(
		vec![
			TermLogger::new(LevelFilter::Info, logger_config, TerminalMode::Mixed, ColorChoice::Auto),
			// TermLogger::new(LevelFilter::Debug, Config::default(), TerminalMode::Mixed, ColorChoice::Auto),
			// WriteLogger::new(LevelFilter::Error, Config::default(), File::create("my_rust_binary.log").unwrap()),
		]
	).unwrap();

    let keep_going = Arc::new(AtomicBool::new(true));
    let keep_going_flag = keep_going.clone();
    let watch_for_quit_handle = thread::spawn(move || {watch_for_quit(keep_going_flag);});

	// let mut files_sets:Vec<FilesSet> = Vec::new();
	// setup_files_sets(&mut files_sets);

	let files_sets:Vec<FilesSet> = config::get_file_sets(None);

	let mut threads_handles: Vec<JoinHandle<()>> = Vec::new();
	for files_set in files_sets {
		let keep_going_flag = keep_going.clone();
		let join_handle = thread::spawn(move || {update_fileset(keep_going_flag, files_set);});
		threads_handles.push(join_handle);
	}

	for thread_handle in threads_handles {
		if let Err(e) = thread_handle.join() {
			error!("thread join error: {:?}", e);
		}
	}

	keep_going.store(false, Ordering::Relaxed);
	if let Err(e) = watch_for_quit_handle.join() {
		error!("watch_for_quit thread join error: {:?}", e);
	}
}

#[derive(Debug)]
struct FileToScan {
	path: PathBuf,
	mdate: DateTime<Local>,
	size: u64,
}

fn update_fileset(keep_going: Arc<AtomicBool>, files_set: FilesSet) {
	// println!("{:?}", files_set);

	if let Err(e) = initalise_database(&files_set) {
		panic!("Error initialising main db: {}", e)
	}

	let mut db_path_main = files_set.db_dir.clone();
	db_path_main.push(sqlstatements::MAIN_DB);
	let mut db_path_metadata = files_set.db_dir.clone();
	db_path_metadata.push(sqlstatements::METADATA_DB);
	let mut db_path_contents = files_set.db_dir.clone();
	db_path_contents.push(sqlstatements::CONTENT_DB);

    info!("{}: Starting to traverse directory: {:?}", files_set.name, files_set.local_root_path);
	//let p = PathBuf::new();
	//p.metadata()?.modified()

	//first build a list of files, then process them in order of modified date desc.
	let mut files_to_scan:Vec<FileToScan> = Vec::new();
	let mut max_folder_depth:usize = std::usize::MAX;
	if !files_set.include_subdirs {
		max_folder_depth = 1;
	}
	let include_regex: Option<Regex>;
	if let Some(include_regex_str) = files_set.include_regex {
		include_regex = Some(Regex::new(&include_regex_str).unwrap());
	} else {
		include_regex = None
	}
	let exclude_regex: Option<Regex>;
	if let Some(exclude_regex_str) = files_set.exclude_regex {
		exclude_regex = Some(Regex::new(&exclude_regex_str).unwrap());
	} else {
		exclude_regex = None
	}
	let dt_from:Option<DateTime<Local>>;
	if let Some(naive_dt_from) = files_set.dt_from {
		dt_from = Some(naivedate_to_local(naive_dt_from));
	} else {
		dt_from = None;
	}
	let dt_to:Option<DateTime<Local>>;
	if let Some(naive_dt_to) = files_set.dt_to {
		dt_to = Some(naivedate_to_local(naive_dt_to));
	} else {
		dt_to = None;
	}
	// println!("max_folder_depth: {}", max_folder_depth);
	for entry in WalkDir::new(&files_set.local_root_path)
		.max_depth(max_folder_depth)
		.into_iter()
		.filter_map(|e| e.ok()) // Skip errors
	{
		let path = entry.path();

		// info!("{} files scanned, {} files included", files_set.name, files_set.local_root_path);
		
        // Process only files (not directories)
		if path.is_file() {
			// println!("{:?}", path);
			match path.metadata() {
				Ok(path_metadata) => {
					match path_metadata.modified() {
						Ok(file_mtime) => {
							let filetimelocal: DateTime<Local> = file_mtime.into();
							let mut include_file = true;
							if let Some(dt_from) = dt_from {
								if filetimelocal <= dt_from {
									include_file = false;
								}
							}
							if include_file && let Some(dt_to) = dt_to {
								if filetimelocal >= dt_to {
									include_file = false;
								}
							}
							if include_file && let Some(include_regex) = &include_regex {
								if !include_regex.is_match(&path.to_string_lossy()) {
									include_file = false;
								}
							}
							if include_file && let Some(exclude_regex) = &exclude_regex {
								if exclude_regex.is_match(&path.to_string_lossy()) {
									include_file = false;
								}
							}
							if include_file {
								files_to_scan.push(
									FileToScan {
										path: path.to_path_buf(),
										mdate: filetimelocal,
										size: path_metadata.len(),
									}
								);
							}
						}
						Err(e) => {
							panic!("Error getting file modified time: {:?}", e);
						}
					}
				}
				Err(e) => {
					panic!("Could not get file metadata: {}", e);
				}
			}
		}

		if !keep_going.load(Ordering::Relaxed) {
			break;
		}
    }

	//sort by file date desc
	files_to_scan.sort_unstable_by_key(|f| Reverse(f.mdate));
	// println!("{:#?}", files_to_scan);

    info!("{}: Finished traversing directory {:?}", files_set.name, files_set.local_root_path);

	info!("{}: Extracting text from {} files", files_set.name, files_to_scan.len());

	//flag all files to be deleted. Then unflag them 1-by-1.
	for (ifile, file_to_scan) in files_to_scan.iter().enumerate() {
		//if the drive or network path have disconnected, then exit now.
		if !files_set.local_root_path.exists() {
			error!("Error accessing path {:?}. Exiting early.", files_set.local_root_path);
			break;
		}
		if file_to_scan.path.exists() {
			let filetimeutc: DateTime<Utc> = file_to_scan.mdate.into();
			let filetimelocal: DateTime<Local> = file_to_scan.mdate.into();
			//let filetimeunix: i64 = file_to_scan.mdate.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64; //i64 not u64 so this can go into sqlite
			let filetimeunix: i64 = filetimeutc.timestamp();
			info!("{}: {} ({}/{}) {}", files_set.name, filetimelocal.format("%Y-%m-%d %H:%M:%S"), ifile+1, files_to_scan.len(), file_to_scan.path.to_string_lossy());
			let parent_filename = file_to_scan.path.file_name().unwrap().to_string_lossy().to_string();
			let relative_path = path_to_agnostic_relative(&file_to_scan.path.parent().unwrap(), &files_set.local_root_path);
			// println!("relative_path {}", relative_path);
			let parent_crc: i64 = checksum_file(Crc64Nvme, &file_to_scan.path.to_string_lossy(), None).unwrap() as i64;
			//fill up pre-scanned files. Need filename, parent_files, crc
			let mut pre_scanned_items: Vec<FileListItem> = Vec::new();
			//get top_parent_rid
			let sql = format!("SELECT rid, crc FROM f WHERE filename = {} AND path = {} AND depth=0", dbfmt_t(&parent_filename), dbfmt_t(&relative_path));
			match query_single_row_to_tuple::<(i64, i64)>(&db_path_metadata, &sql) {
				Ok(top_parent_row) => {
					if top_parent_row.is_some() {
						let top_parent_crc = top_parent_row.unwrap().1;
						if top_parent_crc==parent_crc {
							info!("{}: CRC match, so skip.", files_set.name);
							continue;
						}
						let top_parent_rid = top_parent_row.unwrap().0;
						let sql = format!("SELECT rid, filename, depth, parent_rid, crc FROM f WHERE top_parent_rid = {} ORDER BY rid", top_parent_rid);
						match query_to_tuples::<(i64, String, i64, Option<i64>, i64)>(&db_path_metadata, &sql) {
							Ok(rows) => {
								// println!("rows:\n{:#?}", rows);
								for row in rows.iter() {
									let filename = row.1.to_string();
									let mut parent_rid = row.3;
									let crc = row.4;
									let mut parent_files: Vec<String> = Vec::new();
									//build up parent files
									while parent_rid.is_some() {
										for row2 in rows.iter() {
											let rid2 = row2.0;
											if rid2==parent_rid.unwrap() {
												let filename2 = row2.1.to_string();
												parent_rid = row2.3;
												parent_files.push(filename2);
												break;
											}
										}
									}
									parent_files.reverse();
									pre_scanned_items.push(FileListItem { filename: filename, parent_files: parent_files, crc: crc, size: -1, text_contents: None });
								}
							}
							Err(e) => {
								panic!("Error fetching pre scanned items: {}", e);
							}
						}
					}
				}
				Err(e) => {
					panic!("Error fetching top_parent_rid: {}", e);
				}
			}
			// println!("pre_scanned_items:\n{:#?}", pre_scanned_items);
			let keep_going_flag = keep_going.clone();
			match extract_text_from_file(file_to_scan.path.as_path(), pre_scanned_items, keep_going_flag) {
				Ok(contents) => {
					// println!("{:#?}", contents);
					//always at least one file, even if empty
					if contents.is_empty() {
						panic!("Unexpected empty Vec<FileListItem> from extract_text_from_file()");
					}
					//first item is always parent, with 0 parent files
					let parent_item = &contents[0];
					if !parent_item.parent_files.is_empty() {
						panic!("Unexpected parent_files in parent item from extract_text_from_file(): {:#?}", parent_item);
					}

					//build up a heirarchy of links
					// let mut file_links: Vec<(i64, i64)> = Vec::new(); //links to contents-index and db-rid
					let mut links:HashMap<Vec<String>, (usize, i64)> = HashMap::new(); //links to contents-index and db-rid
					for (ic, file_content) in contents.iter().enumerate(){
						let parent_files = file_content.parent_files.to_vec();
						if ic>0 && !links.contains_key(&parent_files) {
							//find parent
							let mut found_parent_file_content:bool = false;
							for (ic2, file_content2) in contents.iter().enumerate(){
								if ic2 >= ic {
									break;
								}
								// println!("file_content2: {:?}", file_content2);
								// println!("current filename and parent_files: {}, {:?}", file_content.filename, file_content.parent_files);
								let expected_filename = file_content.parent_files.last().unwrap();
								let expected_parent_files = &file_content.parent_files[0..file_content.parent_files.len()-1];
								// println!("expected_filename: {}, comparison_filename: {}", expected_filename, file_content2.filename);
								// println!("expected_parent_files: {:?}, comparison parent_files {:?}", expected_parent_files, file_content2.parent_files);
								if file_content2.filename==*expected_filename && file_content2.parent_files == expected_parent_files {
									links.insert(parent_files.to_vec(), (ic2, -1));
									// println!("links: {:#?}", links);
									found_parent_file_content = true;
								}
							}
							if !found_parent_file_content {
								panic!("Parent FileListItem not found for file {:?}", file_to_scan.path)
							}
						}
					}
					// println!("{:#?}", links);
					
					//update db
					let mut top_parent_rid: i64 = -1;
					let mut frid: i64;
					for file_content in contents {
						// this is in order of adding, so the files in parent files will always exist, and top parent is always first
						let depth = file_content.parent_files.len();
						// find parent_rid
						let mut parent_rid: Option<i64> = None;
						if depth>0 {
							let link = links.get(&file_content.parent_files).unwrap();
							if link.1==-1 {
								let filename = file_content.parent_files.last().unwrap().to_owned();
								let relative_path = relative_path.to_owned();
								let sql = where_sql!("SELECT rid FROM f WHERE {} AND {} AND {}",
									("filename", dbfmt_comp(Some(filename), CompOp::Eq)),
									("path", dbfmt_comp(Some(relative_path), CompOp::Eq)),
									("depth", dbfmt_comp(Some(depth-1), CompOp::Eq))
								);
								match query_to_i64(&db_path_metadata, &sql) {
									Ok(rid) => {
										if rid.is_none() {
											panic!("Couldn't retrieve parent_rid for\n{}", sql);
										}
										parent_rid = rid;
										links.insert(file_content.parent_files.to_vec(), (link.0, parent_rid.unwrap()));
									}
									Err(e) => {
										panic!("Error fetching parent_rid: {}", e);
									}
								}
							} else {
								parent_rid = Some(link.1);
							}
						}
						info!("{}:  {}: {:?}", files_set.name, file_content.filename, file_content.parent_files);
						//does this item exist in db?
						let sql = where_sql!("SELECT rid, crc, time FROM f WHERE {} AND {} AND {}",
							("parent_rid", dbfmt_comp(parent_rid, CompOp::Eq)),
							("filename", dbfmt_comp(Some(file_content.filename.to_string()), CompOp::Eq)),
							("path", dbfmt_comp(Some(relative_path.to_owned()), CompOp::Eq))
						);
						match query_single_row_to_tuple::<(i64, i64, i64)>(&db_path_metadata, &sql) {
							Ok(row) => {
								match row {
									Some((rid, crc, ftime)) => {
										//row exists, does the data need updating?
										if crc != file_content.crc {
											info!("{}:     {} has different crc, need to update database.", files_set.name, file_content.filename);
											if file_content.text_contents.is_none() {
												panic!("file has different crc, need to update database.\nBUT file_content.text_contents is None!");
											}
											//meta
											let conn = Connection::open(&db_path_metadata).unwrap();
											let sql = format!("UPDATE f SET size={}, time={}, crc={} WHERE rid={}",
												dbfmt_t(&file_content.size),
												dbfmt_t(&filetimeunix),
												dbfmt_t(&file_content.crc),
												dbfmt_t(&rid)
											);
											if let Err(e) = conn.execute(&sql, []) {
												panic!("Could not update f at rid={}.\n{}", rid, e);
											}
											//main
											let conn = Connection::open(&db_path_main).unwrap();
											let sql = format!("UPDATE fsearch SET modified_utc={} WHERE frid = {}",
												dbfmt_t(&filetimeutc),
												dbfmt_t(&rid),
											);
											if let Err(e) = conn.execute(&sql, []) {
												panic!("Could not update fsearch at frid={}.\n{}", rid, e);
											}
											//contents
											let conn = Connection::open(&db_path_contents).unwrap();
											let sql = format!("UPDATE t SET contents = {} WHERE frid = {}",
												dbfmt(file_content.text_contents.map(|s| s.to_lowercase())),
												dbfmt_t(&rid)
											);
											if let Err(e) = conn.execute(&sql, []) {
												panic!("Could not update contents at frid={}.\n{}", rid, e);
											}
										} else if ftime != filetimeunix {
											info!("{}:     {} has same crc but filetime is different, only update timestamp.", files_set.name, file_content.filename);
											//meta
											let conn = Connection::open(&db_path_metadata).unwrap();
											let sql = format!("UPDATE f SET time={} WHERE rid={}",
												dbfmt_t(&filetimeunix),
												dbfmt_t(&rid)
											);
											if let Err(e) = conn.execute(&sql, []) {
												panic!("Could not update f at rid={}.\n{}", rid, e);
											}
											//main
											let conn = Connection::open(&db_path_main).unwrap();
											let sql = format!("UPDATE fsearch SET modified_utc={} WHERE frid = {}",
												dbfmt_t(&filetimeutc),
												dbfmt_t(&rid),
											);
											if let Err(e) = conn.execute(&sql, []) {
												panic!("Could not update fsearch at frid={}.\n{}", rid, e);
											}
										} else {
											info!("{}:    {} has same crc, no need to update database.", files_set.name, file_content.filename);
										}
									}
									None => {
										//item doesn't exist in database, insert new row
										info!("{}:     file is not in database, inserting.", files_set.name);
										//meta
										let conn = Connection::open(&db_path_metadata).unwrap();
										let sql = format!("INSERT INTO f (filename,path,size,time,crc,depth,parent_rid) VALUES ({},{},{},{},{},{},{})",
											dbfmt_t(&file_content.filename),
											dbfmt_t(&relative_path),
											dbfmt_t(&file_content.size),
											dbfmt_t(&filetimeunix),
											dbfmt_t(&file_content.crc),
											dbfmt_t(&depth),
											dbfmt(parent_rid)
										);
										if let Err(e) = conn.execute(&sql, []) {
											panic!("Could not insert new row into f.\n{}", e);
										}
										//get frid
										frid = conn.last_insert_rowid();
										if depth==0 {
											top_parent_rid = frid;
										}
										let sql = format!("UPDATE f SET top_parent_rid = {} WHERE rid = {}", top_parent_rid, frid);
										if let Err(e) = conn.execute(&sql, []) {
											panic!("Could not insert new row into f.\n{}", e);
										}
										//main
										let conn = Connection::open(&db_path_main).unwrap();
										let file_extension = file_content.filename.to_lowercase().split('.').last().unwrap().to_string();
										let sql = format!("INSERT INTO fsearch (frid,filename_search,path_search,modified_utc,filename_ext) VALUES ({},{},{},{},{})",
											dbfmt_t(&frid),
											dbfmt_t(&file_content.filename.to_lowercase()),
											dbfmt_t(&relative_path.to_lowercase()),
											dbfmt_t(&filetimeutc),
											dbfmt_t(&file_extension),
										);
										if let Err(e) = conn.execute(&sql, []) {
											panic!("Could not insert new row into fsearch.\n{}", e);
										}
										//contents
										let conn = Connection::open(&db_path_contents).unwrap();
										let sql = format!("INSERT INTO t (frid,contents) VALUES ({},{})",
											dbfmt_t(&frid),
											dbfmt(file_content.text_contents.map(|s| s.to_lowercase()))
										);
										if let Err(e) = conn.execute(&sql, []) {
											panic!("Could not insert new row into contents.\n{}", e);
										}
									}
								}
							}
							Err(e) => {
								panic!("Error running query: {}\n{}", e, sql);
							}
						}
					}
				}
				Err(e) => {
					panic!("Error extracting text: {}", e);
				}
			}

		}
		if !keep_going.load(Ordering::Relaxed) {
			break;
		}
	}

	info!("{}: END of files_set: {:?}", files_set.name, files_set.name);
}
