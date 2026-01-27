//db_ver
// <=3 - initial design and development
// x = initial release

use crate::{config::FilesSet, sqlstatements::DB_VER};


pub fn main_db() -> Vec<String> {
	let mut sqls:Vec<String> = Vec::new();

	sqls.push(String::from(r#"
CREATE TABLE IF NOT EXISTS settings
(
setting_name TEXT UNIQUE
,setting_value TEXT
);
"#));
	sqls.push(String::from(format!(r#"
INSERT INTO settings (setting_name,setting_value) VALUES ('db_ver','{}');
"#, DB_VER)));

	sqls.push(String::from(r#"
CREATE TABLE IF NOT EXISTS fsearch
(
frid INT
,filename_search TEXT
,path_search TEXT
,modified_utc TEXT
,filename_ext TEXT
);
"#));

	sqls.push(String::from(r#"CREATE UNIQUE INDEX IF NOT EXISTS idx_fsearch_frid ON fsearch(frid);"#));

	return  sqls;
}

pub fn insert_settings(files_set: &FilesSet) -> Vec<String> {
	let mut sqls:Vec<String> = Vec::new();

	sqls.push(String::from(format!(r#"
INSERT INTO settings (setting_name,setting_value) SELECT 'name','{}'
WHERE NOT EXISTS (SELECT 1 FROM settings s2 WHERE s2.setting_name = 'name')
"#, files_set.name.replace("'", "''"))));

	sqls.push(String::from(format!(r#"
INSERT INTO settings (setting_name,setting_value) SELECT 'root_path','{}'
WHERE NOT EXISTS (SELECT 1 FROM settings s2 WHERE s2.setting_name = 'root_path')
"#, files_set.local_root_path.to_string_lossy().replace("'", "''"))));

return sqls;
}

pub fn metadata_db() -> Vec<String> {
	let mut sqls:Vec<String> = Vec::new();

	sqls.push(String::from(r#"
CREATE TABLE IF NOT EXISTS f (
rid INTEGER PRIMARY KEY AUTOINCREMENT
,filename TEXT
,path TEXT
,size INT
,time INT
,crc INT
,depth INT
,parent_rid INT
,top_parent_rid INT
);
"#));

	sqls.push(String::from(r#"CREATE INDEX IF NOT EXISTS idx_f_filename_path ON f(filename,path);"#));
	sqls.push(String::from(r#"CREATE INDEX IF NOT EXISTS idx_f_parent_filename ON f(parent_rid,filename);"#));
	sqls.push(String::from(r#"CREATE INDEX IF NOT EXISTS idx_f_top_parent_rid ON f(top_parent_rid);"#));

	sqls.push(String::from(r#"
CREATE TABLE IF NOT EXISTS fdel (
frid INTEGER
,filename TEXT
,path TEXT
);
"#));

	sqls.push(String::from(r#"CREATE INDEX IF NOT EXISTS idx_fdel_frid ON fdel(frid);"#));

	sqls.push(String::from(r#"
CREATE TABLE IF NOT EXISTS flddel (
path TEXT
);
"#));

return  sqls;
}

pub fn content_db() -> Vec<String> {
	let mut sqls:Vec<String> = Vec::new();

	sqls.push(String::from(r#"
CREATE TABLE IF NOT EXISTS t (
frid INT
,contents TEXT
);
"#));

	sqls.push(String::from(r#"CREATE UNIQUE INDEX IF NOT EXISTS idx_t_frid ON t(frid);"#));

	sqls.push(String::from(r#"
CREATE VIRTUAL TABLE IF NOT EXISTS ti USING fts5(contents, content='t', content_rowid='frid');
"#));

	sqls.push(String::from(r#"
CREATE TRIGGER IF NOT EXISTS t_ai AFTER INSERT ON t BEGIN
  INSERT INTO ti(rowid, contents) VALUES (new.frid, new.contents);
END;
"#));

	sqls.push(String::from(r#"
CREATE TRIGGER IF NOT EXISTS t_ad AFTER DELETE ON t BEGIN
  INSERT INTO ti(ti, rowid, contents) VALUES('delete', old.frid, old.contents);
END;
"#));

	sqls.push(String::from(r#"
CREATE TRIGGER IF NOT EXISTS t_au AFTER UPDATE ON t BEGIN
  INSERT INTO ti(ti, rowid, contents) VALUES('delete', old.frid, old.contents);
  INSERT INTO ti(rowid, contents) VALUES (new.frid, new.contents);
END;
"#));

	return  sqls;
}
