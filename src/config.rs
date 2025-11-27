use chrono::NaiveDate;
use log::*;
use serde::Deserialize;
use std::{fs, path::{Path, PathBuf}};

const APP_NAME: &str = "file_searcher_deamon_v5";

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub global: GlobalConfig,
    pub file_set: Vec<FilesSet>, // 'file_set' is the array name
}

#[derive(Deserialize, Debug)]
pub struct GlobalConfig {
    pub standard_file_exclusion: String
}

#[derive(Clone, Deserialize, Debug)]
pub struct FilesSet {
	pub name: String,
	pub local_root_path: PathBuf,
	pub include_subdirs: bool,
	pub include_regex: Option<String>,
	pub exclude_regex: Option<String>,
    #[serde(default)]
    #[serde(with = "toml_datetime_compat")]
	pub dt_from: Option<NaiveDate>,
    #[serde(default)]
    #[serde(with = "toml_datetime_compat")]
	pub dt_to: Option<NaiveDate>,
	pub db_dir: PathBuf,
}

fn get_app_config_path(app_name: &str) -> Option<PathBuf> {
    if let Some(config_dir) = dirs::config_dir() {
        let app_config_path = config_dir.join(app_name);
        Some(app_config_path)
    } else {
        None
    }
}

fn get_config_file_path() -> PathBuf {
	let app_config_path = get_app_config_path(APP_NAME).expect("could not resolve app config path");
	return app_config_path.join("config.toml");
}

/// If config_file_path is None, local user path is used as defined in dirs::config_dir(), also paths ./config.toml and ./tests/config.toml are checked
pub fn get_file_sets(mut config_file_path: Option<PathBuf>) -> Vec<FilesSet> {
	if config_file_path.is_none() {
		config_file_path = Some(get_config_file_path());
	}
	let mut config_file_path = config_file_path.unwrap();
	if !config_file_path.exists() {
		//try the tests path
		debug!("Config file does not exist at {}\nSee ./tests/config.toml for example", config_file_path.to_string_lossy());
		config_file_path = PathBuf::from("config.toml");
		if !config_file_path.exists() {
			config_file_path = PathBuf::from("./tests/config.toml");
			if !config_file_path.exists() {
				panic!("Config file does not exist at common locations.\nSee ./tests/config.toml for example");
			}
		}
	}
	let toml_content = fs::read_to_string(config_file_path).unwrap();
    let app_config: AppConfig = toml::from_str(&toml_content).expect("Failed to parse TOML");
	let mut file_sets: Vec<FilesSet> = Vec::new();
    
	//replace standard_file_exclusion
	for file_set in app_config.file_set {
		let mut new_file_set:FilesSet = file_set.clone();
		let exclude_regex:Option<String>;
		if new_file_set.exclude_regex.unwrap_or(String::new())=="standard_file_exclusion" {
			exclude_regex = Some(app_config.global.standard_file_exclusion.to_string());
		} else {
			exclude_regex = file_set.exclude_regex;
		}
		new_file_set.exclude_regex = exclude_regex;
		file_sets.push(new_file_set);
	}

	return file_sets;
}
