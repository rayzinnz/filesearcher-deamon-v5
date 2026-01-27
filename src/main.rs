use async_walkdir::WalkDir;
use chrono::{DateTime, Local, Utc};
use crc_fast::{checksum_file, CrcAlgorithm::Crc64Nvme};
use extract_text::*;
use futures_lite::future::block_on;
use futures_lite::stream::StreamExt;
use helper_lib::{
	self,
	setup_logger,
	watch_for_quit,
	where_sql,
	datetime::naivedate_to_local,
	paths::{format_bytes, path_to_agnostic_relative},
	sql::{CompOp, dbfmt, dbfmt_comp, dbfmt_t, query_single_row_to_tuple, query_to_i64, query_to_tuples}
};
use log::*;
use regex::Regex;
use rusqlite::{Connection};
use std::{
	cmp::Reverse, collections::{HashMap, HashSet}, error::Error, fs, path::PathBuf, sync::{Arc, atomic::{AtomicBool, Ordering}}, thread::{self, JoinHandle}
};
// use walkdir::WalkDir;

pub mod config;
use config::FilesSet;
pub mod sqlstatements;
use sqlstatements::sql_initialise;

fn main() {
	setup_logger(LevelFilter::Info);

    let keep_going = Arc::new(AtomicBool::new(true));
    let keep_going_flag = keep_going.clone();
    let _watch_for_quit_handle = thread::spawn(move || {watch_for_quit(keep_going_flag);});

	let files_sets:Vec<FilesSet> = config::get_file_sets(None);

	let mut threads_handles: Vec<JoinHandle<()>> = Vec::new();
	for files_set in files_sets {
		// println!("{:?}", files_set.exclude_regex);
		let keep_going_flag = keep_going.clone();
		let join_handle = thread::Builder::new()
			.name(files_set.name.to_string())
			.spawn(move || {update_fileset(keep_going_flag, files_set);})
			.expect("Failed to spawn thread");
		threads_handles.push(join_handle);
	}

	for thread_handle in threads_handles {
		if let Err(e) = thread_handle.join() {
			error!("thread join error: {:?}", e);
		}
	}

	keep_going.store(false, Ordering::Relaxed);
	#[cfg(target_os = "linux")]
	if let Err(e) = _watch_for_quit_handle.join() {
		error!("watch_for_quit thread join error: {:?}", e);
	}
}

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
				conn.execute_batch(&sql)?;
			}
		}
		{
			let conn = Connection::open(&db_path_metadata)?;
			let sqls = sql_initialise::metadata_db();
			for sql in sqls {
				let _changes = conn.execute_batch(&sql)?;
			}
		}
		{
			let conn = Connection::open(&db_path_contents)?;
			let sqls = sql_initialise::content_db();
			for sql in sqls {
				let _changes = conn.execute_batch(&sql)?;
			}
		}
	}

	//run settings inserts
	{
		let conn = Connection::open(&db_path_main)?;
		let sqls = sql_initialise::insert_settings(&files_set);
		for sql in sqls {
			conn.execute_batch(&sql)?;
		}
	}

	Ok(())

}

#[derive(Debug)]
struct FileToScan {
	path: PathBuf,
	mdate: DateTime<Local>,
	size: u64,
}

fn get_file_listing(keep_going: Arc<AtomicBool>, files_set: &FilesSet) -> Vec<FileToScan> {
    info!("{}: Starting to traverse directory: {:?}", files_set.name, files_set.local_root_path);
	//let p = PathBuf::new();
	//p.metadata()?.modified()

	let mut files_to_scan:Vec<FileToScan> = Vec::new();
	// let mut max_folder_depth:usize = std::usize::MAX;
	// if !files_set.include_subdirs {
	// 	max_folder_depth = 1;
	// }
	let include_regex: Option<Regex>;
	if let Some(include_regex_str) = files_set.include_regex.clone() {
		include_regex = Some(Regex::new(&include_regex_str).unwrap());
	} else {
		include_regex = None
	}
	let exclude_regex: Option<Regex>;
	if let Some(exclude_regex_str) = files_set.exclude_regex.clone() {
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
	let mut file_count:usize = 0;

	if files_set.include_subdirs {
		block_on(async {
			let mut entries  = WalkDir::new(files_set.local_root_path.clone());
			loop {
				match entries.next().await {
					Some(Ok(entry)) => {
						let path = entry.path();
						// println!("{}", path.to_string_lossy());
						file_count += 1;
						// println!("{} {}", file_count, path.to_string_lossy());
						// info!("{} files scanned, {} files included", files_set.name, files_set.local_root_path);
						if file_count % 10000 == 0 {
							info!("{}: scanned {} files", files_set.name, file_count);
						}
						match entry.metadata().await {
							Ok(path_metadata) => {
								if path_metadata.is_file() {
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
											// if path.to_string_lossy().contains("flycheck") {
											// 	println!("{}", path.to_string_lossy());
											// 	println!("{}", include_file);
											// }
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
											keep_going.store(false, Ordering::Relaxed);
											panic!("Error getting file modified time: {:?}", e);
										}
									}
								
								}
							}
							Err(e) => {
								keep_going.store(false, Ordering::Relaxed);
								panic!("Could not get file metadata: {}", e);
							}
						}

						if !keep_going.load(Ordering::Relaxed) {
							break;
						}

					},
					Some(Err(e)) => {
						error!("error: {}", e);
						break;
					}
					None => break,
				}
			}
		});
	} else {
		let runtime = tokio::runtime::Runtime::new().expect("Error starting tokio::runtime");
		runtime.block_on( async {
			let mut entries = tokio::fs::read_dir(files_set.local_root_path.clone()).await.expect("Error with tokio::fs::read_dir");
			// while let Some(entry) = entries.next_entry().await.unwrap() {
			// 	let path = entry.path();
			// 	let path_metadata = entry.metadata().await.unwrap();
			// 	// println!("{}", path.to_string_lossy());
			// 	if path_metadata.is_file() {
			// 		// println!("{}", path.to_string_lossy());
			// 		files.push(path);
			// 	}
			// }

			loop {
				match entries.next_entry().await {
					Ok(Some(entry)) => {
						let path = entry.path();
						// println!("{}", path.to_string_lossy());
						file_count += 1;
						// println!("{} {}", file_count, path.to_string_lossy());
						// info!("{} files scanned, {} files included", files_set.name, files_set.local_root_path);
						if file_count % 10000 == 0 {
							info!("{}: scanned {} files", files_set.name, file_count);
						}
						match entry.metadata().await {
							Ok(path_metadata) => {
								if path_metadata.is_file() {
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
											keep_going.store(false, Ordering::Relaxed);
											panic!("Error getting file modified time: {:?}", e);
										}
									}
								
								}
							}
							Err(e) => {
								keep_going.store(false, Ordering::Relaxed);
								panic!("Could not get file metadata: {}", e);
							}
						}

						if !keep_going.load(Ordering::Relaxed) {
							break;
						}

					},
					Err(e) => {
						error!("error: {}", e);
						break;
					}
					Ok(None) => break,
				}
			}
		});
	}

	files_to_scan
}

fn update_fileset(keep_going: Arc<AtomicBool>, files_set: FilesSet) {
	// println!("{:?}", files_set);

	if let Err(e) = initalise_database(&files_set) {
		keep_going.store(false, Ordering::Relaxed);
		panic!("Error initialising main db: {}", e)
	}

	let mut db_path_main = files_set.db_dir.clone();
	db_path_main.push(sqlstatements::MAIN_DB);
	let mut db_path_metadata = files_set.db_dir.clone();
	db_path_metadata.push(sqlstatements::METADATA_DB);
	let mut db_path_contents = files_set.db_dir.clone();
	db_path_contents.push(sqlstatements::CONTENT_DB);

	//first build a list of files, then process them in order of modified date desc.
	let keep_going_flag = keep_going.clone();
	let mut files_to_scan = get_file_listing(keep_going_flag, &files_set);

	// return;

	//sort by file date desc
	files_to_scan.sort_unstable_by_key(|f| Reverse(f.mdate));
	// println!("{:#?}", files_to_scan);

    info!("{}: Finished traversing directory {:?}", files_set.name, files_set.local_root_path);

	info!("{}: Extracting text from {} files", files_set.name, files_to_scan.len());

	//flag all files to be deleted. Then unflag them 1-by-1.
	{
		let conn = Connection::open(&db_path_metadata).expect("cannot connect to meta db");
		let sql = "DELETE FROM fdel;";
		conn.execute_batch(sql).expect("Error deleting from fdel");
		let sql = "DELETE FROM flddel;";
		conn.execute_batch(sql).expect("Error deleting from flddel");
		let sql = "INSERT INTO fdel(frid) SELECT rid FROM f;";
		conn.execute_batch(sql).expect("Error inserting frid to temp table fdel");
	}
	let mut fdel_statements:Vec<String> = Vec::new();
	let runtime = tokio::runtime::Runtime::new().expect("Error starting tokio::runtime");
	runtime.block_on( async {
		for (ifile, file_to_scan) in files_to_scan.iter().enumerate() {
			if !keep_going.load(Ordering::Relaxed) {
				break;
			}
			// if file_to_scan.path.exists() {
			if tokio::fs::try_exists(&file_to_scan.path).await.unwrap_or_default() {
				let filetimeutc: DateTime<Utc> = file_to_scan.mdate.into();
				let filetimelocal: DateTime<Local> = file_to_scan.mdate.into();
				//let filetimeunix: i64 = file_to_scan.mdate.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64; //i64 not u64 so this can go into sqlite
				let filetimeunix: i64 = filetimeutc.timestamp();
				trace!("{}: {} ({}/{}) {} ({})", files_set.name, filetimelocal.format("%Y-%m-%d %H:%M:%S"), ifile+1, files_to_scan.len(), file_to_scan.path.to_string_lossy(), format_bytes(file_to_scan.size));
				if (ifile+1) % 10000 ==0 {
					info!("{}: {} ({}/{}) {} ({})", files_set.name, filetimelocal.format("%Y-%m-%d %H:%M:%S"), ifile+1, files_to_scan.len(), file_to_scan.path.to_string_lossy(), format_bytes(file_to_scan.size));
				}
				let parent_filename = file_to_scan.path.file_name().unwrap().to_string_lossy().to_string();
				let relative_path = path_to_agnostic_relative(&file_to_scan.path.parent().unwrap(), &files_set.local_root_path);
				// println!("relative_path {}", relative_path);
				//fill up pre-scanned files. Need filename, parent_files, crc
				let mut pre_scanned_items: Vec<FileListItem> = Vec::new();
				//get top_parent_rid
				let sql = format!("SELECT rid, crc, size, time FROM f WHERE filename = {} AND path = {} AND depth=0", dbfmt_t(&parent_filename), dbfmt_t(&relative_path));
				match query_single_row_to_tuple::<(i64, i64, i64, i64)>(&db_path_metadata, &sql) {
					Ok(top_parent_row) => {
						if top_parent_row.is_some() {
							let top_parent_rid = top_parent_row.unwrap().0;
							fdel_statements.push(format!("DELETE FROM fdel WHERE frid IN (SELECT rid FROM f WHERE top_parent_rid = {});", top_parent_rid));
							let top_parent_size: u64 = top_parent_row.unwrap().2 as u64;
							let top_parent_time = top_parent_row.unwrap().3;
							if top_parent_size==file_to_scan.size && top_parent_time==filetimeunix {
								trace!("{}: size and mdate match, so skip.", files_set.name);
								continue;
							}
							let top_parent_crc = top_parent_row.unwrap().1;
							let parent_crc: i64 = checksum_file(Crc64Nvme, &file_to_scan.path.to_string_lossy(), None).unwrap() as i64;
							if top_parent_crc==parent_crc {
								trace!("{}: CRC match, so skip.", files_set.name);
								continue;
							} else {
								info!("{}: {} ({}/{}) {} ({})", files_set.name, filetimelocal.format("%Y-%m-%d %H:%M:%S"), ifile+1, files_to_scan.len(), file_to_scan.path.to_string_lossy(), format_bytes(file_to_scan.size));
							}
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
									keep_going.store(false, Ordering::Relaxed);
									panic!("Error fetching pre scanned items: {}", e);
								}
							}
						} else {
							info!("{}: {} ({}/{}) {} ({})", files_set.name, filetimelocal.format("%Y-%m-%d %H:%M:%S"), ifile+1, files_to_scan.len(), file_to_scan.path.to_string_lossy(), format_bytes(file_to_scan.size));
						}
					}
					Err(e) => {
						keep_going.store(false, Ordering::Relaxed);
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
							keep_going.store(false, Ordering::Relaxed);
							panic!("Unexpected empty Vec<FileListItem> from extract_text_from_file()");
						}
						//first item is always parent, with 0 parent files
						let parent_item = &contents[0];
						if !parent_item.parent_files.is_empty() {
							keep_going.store(false, Ordering::Relaxed);
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
									keep_going.store(false, Ordering::Relaxed);
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
												keep_going.store(false, Ordering::Relaxed);
												panic!("Couldn't retrieve parent_rid for\n{}", sql);
											}
											parent_rid = rid;
											links.insert(file_content.parent_files.to_vec(), (link.0, parent_rid.unwrap()));
										}
										Err(e) => {
											keep_going.store(false, Ordering::Relaxed);
											panic!("Error fetching parent_rid: {}", e);
										}
									}
								} else {
									parent_rid = Some(link.1);
								}
							}
							trace!("{}:  {}: {:?}", files_set.name, file_content.filename, file_content.parent_files);
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
												trace!("{}:     {} has different crc, need to update database.", files_set.name, file_content.filename);
												if file_content.text_contents.is_none() {
													keep_going.store(false, Ordering::Relaxed);
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
												if let Err(e) = conn.execute_batch(&sql) {
													keep_going.store(false, Ordering::Relaxed);
													panic!("Could not update f at rid={}.\n{}", rid, e);
												}
												//main
												let conn = Connection::open(&db_path_main).unwrap();
												let sql = format!("UPDATE fsearch SET modified_utc={} WHERE frid = {}",
													dbfmt_t(&filetimeutc),
													dbfmt_t(&rid),
												);
												if let Err(e) = conn.execute_batch(&sql) {
													keep_going.store(false, Ordering::Relaxed);
													panic!("Could not update fsearch at frid={}.\n{}", rid, e);
												}
												//contents
												let conn = Connection::open(&db_path_contents).unwrap();
												let sql = format!("UPDATE t SET contents = {} WHERE frid = {}",
													dbfmt(file_content.text_contents),
													dbfmt_t(&rid)
												);
												if let Err(e) = conn.execute_batch(&sql) {
													keep_going.store(false, Ordering::Relaxed);
													panic!("Could not update contents at frid={}.\n{}", rid, e);
												}
											} else if ftime != filetimeunix {
												trace!("{}:     {} has same crc but filetime is different, only update timestamp.", files_set.name, file_content.filename);
												//meta
												let conn = Connection::open(&db_path_metadata).unwrap();
												let sql = format!("UPDATE f SET time={} WHERE rid={}",
													dbfmt_t(&filetimeunix),
													dbfmt_t(&rid)
												);
												if let Err(e) = conn.execute_batch(&sql) {
													keep_going.store(false, Ordering::Relaxed);
													panic!("Could not update f at rid={}.\n{}", rid, e);
												}
												//main
												let conn = Connection::open(&db_path_main).unwrap();
												let sql = format!("UPDATE fsearch SET modified_utc={} WHERE frid = {}",
													dbfmt_t(&filetimeutc),
													dbfmt_t(&rid),
												);
												if let Err(e) = conn.execute_batch(&sql) {
													keep_going.store(false, Ordering::Relaxed);
													panic!("Could not update fsearch at frid={}.\n{}", rid, e);
												}
											} else {
												trace!("{}:    {} has same crc, no need to update database.", files_set.name, file_content.filename);
											}
										}
										None => {
											//item doesn't exist in database, insert new row
											trace!("{}:     file is not in database, inserting.", files_set.name);
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
											if let Err(e) = conn.execute_batch(&sql) {
												keep_going.store(false, Ordering::Relaxed);
												panic!("Could not insert new row into f.\n{}", e);
											}
											//get frid
											frid = conn.last_insert_rowid();
											if depth==0 {
												top_parent_rid = frid;
											}
											let sql = format!("UPDATE f SET top_parent_rid = {} WHERE rid = {}", top_parent_rid, frid);
											if let Err(e) = conn.execute_batch(&sql) {
												keep_going.store(false, Ordering::Relaxed);
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
											if let Err(e) = conn.execute_batch(&sql) {
												keep_going.store(false, Ordering::Relaxed);
												panic!("Could not insert new row into fsearch.\n{}", e);
											}
											//contents
											let conn = Connection::open(&db_path_contents).unwrap();
											// for c in file_content.text_contents.clone().unwrap().as_bytes() { print!("{}-", c) }
											let sql = format!("INSERT INTO t (frid,contents) VALUES ({},{})",
												dbfmt_t(&frid),
												dbfmt(file_content.text_contents)
											);
											if let Err(e) = conn.execute_batch(&sql) {
												keep_going.store(false, Ordering::Relaxed);
												panic!("Could not insert new row into contents.\n{}", e);
											}
										}
									}
								}
								Err(e) => {
									keep_going.store(false, Ordering::Relaxed);
									panic!("Error running query: {}\n{}", e, sql);
								}
							}
						}
					}
					Err(e) => {
						keep_going.store(false, Ordering::Relaxed);
						error!("{}: {} ({}/{}) {} ({})", files_set.name, filetimelocal.format("%Y-%m-%d %H:%M:%S"), ifile+1, files_to_scan.len(), file_to_scan.path.to_string_lossy(), format_bytes(file_to_scan.size));
						panic!("Error extracting text: {}", e);
					}
				}
			} else {
				//if the drive or network path have disconnected, then exit now.
				if !files_set.local_root_path.exists() {
					//this fileset cutoff early, do not consider any deletions
					{
						let conn = Connection::open(&db_path_metadata).expect("cannot connect to meta db");
						let sql = "DELETE FROM fdel;";
						conn.execute_batch(sql).expect("Error deleting from fdel");
						let sql = "DELETE FROM flddel;";
						conn.execute_batch(sql).expect("Error deleting from flddel");
					}
					error!("Error accessing path {:?}. Exiting early.", files_set.local_root_path);
					break;
				}
			}
			if !keep_going.load(Ordering::Relaxed) {
				break;
			}
		}
	});

	if !keep_going.load(Ordering::Relaxed) {
		//if this fileset was cutoff early, do not consider any deletions
		{
			let conn = Connection::open(&db_path_metadata).expect("cannot connect to meta db");
			let sql = "DELETE FROM fdel;";
			conn.execute_batch(sql).expect("Error deleting from fdel");
			let sql = "DELETE FROM flddel;";
			conn.execute_batch(sql).expect("Error deleting from flddel");
		}
	} else {
		//remove existing files from fdel
		{
			let mut conn = Connection::open(&db_path_metadata).expect("cannot connect to meta db");
			info!("{}: start of {} fdel_statements", files_set.name, fdel_statements.len());
			let tx = conn.transaction().expect("could not start transaction");
			for sql in fdel_statements {
				tx.execute_batch(&sql).expect(&format!("error deleting from fdel table, {}\n", sql));
			}
			info!("{}: commit of fdel_statements", files_set.name);
			tx.commit().expect("transaction commit failed");
			info!("{}: end of fdel_statements", files_set.name);
		}
		//delete from dbs table
		{
			let conn = Connection::open(&db_path_metadata).expect("cannot connect to meta db");
			//contents
			conn.execute_batch(&format!("ATTACH DATABASE '{}' AS content;", &db_path_contents.to_string_lossy())).expect("error attaching content db");
			let sql = "DELETE FROM content.t AS t WHERE t.frid IN (SELECT frid FROM fdel)";
			conn.execute_batch(sql).expect("error deleting from content db");
			conn.execute_batch("DETACH DATABASE content;").expect("error detaching content db");
			//main
			conn.execute_batch(&format!("ATTACH DATABASE '{}' AS maindb;", &db_path_main.to_string_lossy())).expect("error attaching main db");
			let sql = "DELETE FROM maindb.fsearch AS fs WHERE fs.frid IN (SELECT frid FROM fdel)";
			conn.execute_batch(sql).expect("error deleting from main db");
			conn.execute_batch("DETACH DATABASE maindb;").expect("error detaching main db");
			//meta
			let sql = "UPDATE fdel
SET (filename, path) =
(
SELECT filename, path
FROM f
WHERE f.rid=fdel.frid
);
";
			conn.execute_batch(sql).expect("error updating fdel filename, path");
			let sql = "DELETE FROM f WHERE f.rid IN (SELECT frid FROM fdel)";
			let rows_deleted = conn.execute(sql, []).expect("error deleting from meta db");
			info!("{}: {} file item rows deleted", files_set.name, rows_deleted);
		}
		//check files are actually deleted, instead of just being excluded from scanning
		let mut removed_dirs: HashSet<String> = HashSet::new();
		let sql = "SELECT f.rid, f.path, f.filename FROM fdel JOIN f ON f.rid=fdel.frid";
		match query_to_tuples::<(i64, String, String)>(&db_path_metadata, &sql) {
			Ok(rows) => {
				//trace!("fdel rows:\n{:#?}", rows);
				info!("{}: fdel row count: {}", files_set.name, rows.len());
				let mut conn = Connection::open(&db_path_metadata).expect("cannot connect to meta db");
				let tx = conn.transaction().expect("could not start transaction");
				for row in rows.iter() {
					let frid = row.0;
					let path = row.1.to_string();
					if removed_dirs.contains(&path) {
						continue;
					}
					let filename = row.2.to_string();
					let dirpath = files_set.local_root_path.join(&path);
					if !dirpath.exists() {
						removed_dirs.insert(path.clone());
						let sql = format!("INSERT INTO flddel (path) VALUES ({})", dbfmt_t(&path));
						tx.execute_batch(&sql).expect(&format!("error inserting into fdel table, {}\n", sql));
					} else {
						let fullpath = dirpath.join(&filename);
						if fullpath.exists() {
//TODO: if file exists, we should delete from fdel, but we should also delete from db's because this may be a new exclusion rule.
							let sql = format!("DELETE FROM fdel WHERE frid = {frid}", );
							tx.execute_batch(&sql).expect(&format!("error deleting from fdel table, {}\n", sql));
						}
					}
				}
				tx.commit().expect("transaction commit failed");
			}
			Err(e) => {
				keep_going.store(false, Ordering::Relaxed);
				panic!("Error fetching pre scanned items: {}", e);
			}
		}
	}

	info!("{}: END of files_set: {:?}", files_set.name, files_set.name);
}
