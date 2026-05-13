use serde::{Serialize, Deserialize};
use core::panic;
use std::io::{Read, Write};
use std::{env::home_dir, sync::LazyLock};
use std::path::Path;
use std::{format, io};
use toml::{to_string_pretty, from_str};
use whoami::username;
use log::{error, info};

/// contains all possible configuration file locations.
/// avalible paths vary based on operating system.
/// the vectors are ordered by precedence: a config file in /opt/* will overrule one in /home/*.
/// if a config file fails to parse, the next highest file will be used.
/// if none of the paths contained here contain a valid configuration file,
/// a blank one will be created from [`Config::default()`].
static CONFIG_PATHS: LazyLock<Vec<String>> = LazyLock::new(|| {
    let user = username().unwrap();
    #[cfg(target_os = "linux")]
    let paths = vec![
        "/opt/hpx-ctl/config.toml".to_string(),
        format!("/home/{user}/hpx-ctl.toml"),
        format!("/home/{user}/.hpx-ctl/config.toml"),
        format!("/home/{user}/.hpx-ctl.toml"),

    ];
    #[cfg(target_os = "windows")]
    let paths = vec![
        "C:/Program Files/hpx-ctl/config.toml".to_string(),
        format!("C:/Users/{user}/.hpx-ctl.toml"),
        format!("C:/Users/{user}/.hpx-ctl/config.toml"),
        format!("C:/Users/{user}/.hpx-ctl.toml"),
    ];
    #[cfg(target_os = "macos")]
    let paths = vec![
        "/opt/hpx-ctl/config.toml".to_string(),
        format!("/Users/{user}/.hpx-ctl.toml"),
        format!("/Users/{user}/.hpx-ctl/config.toml"),
        format!("/Users/{user}/.hpx-ctl.toml"),
    ];
    return paths;
});

/// gets a list of avalible config files based on [`CONFIG_PATHS`].
fn list_avalible_configs() -> Option<Vec<String>> {
    let mut v: Vec<String> = Vec::new();
    for path in CONFIG_PATHS.clone() {
        if std::fs::exists(&path).unwrap() {
            v.push(path.clone());
        }
    }
    if v.len() == 0 {
        return None;
    } else {
        return Some(v)
    }
}

/// write a config file to the first writable path in [`CONFIG_PATHS`].
/// the config will contain the values from the [`Default`] implementation for [`Config`]
fn write_config(path: String) -> io::Result<()> {
    let default_config = Config::default();
    let text = to_string_pretty(&default_config).unwrap();
    let mut config_file = match std::fs::File::create(path) {
        Ok(f) => f,
        Err(why) => {
            error!("Failed to create default configuration file: {why:?}");
            return Err(io::Error::new(io::ErrorKind::Other, "Failed to write default config."))
        }
    };
    config_file.write_all(text.as_bytes())
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub detector_interface_name: String,
    pub server_port: u16,
}
impl Config {
    pub fn load() -> Self {
        info!("Loading config...");
        let config_paths = list_avalible_configs();
        if config_paths.is_none() {
            let p = CONFIG_PATHS[1].clone();
            info!("No config found! Creating one at {p}");
            let _ = write_config(p);
            return Self::default()
        } else {
            let mut config_candidates: Vec<Result<Config,()>> = Vec::new();
            for config_path in config_paths.unwrap() {
                match std::fs::File::open(config_path) {
                    Ok(mut f) => {
                        let mut text: Vec<u8> = Vec::new();
                        match f.read(& mut text) {
                            Ok(_) => {
                                match String::from_utf8(text) {
                                    Ok(s) => {
                                        match from_str(&s) {
                                            Ok(config) => config_candidates.push(Ok(config)),
                                            Err(_) => {
                                                config_candidates.push(Err(()));
                                                continue
                                            },
                                        }
                                    }
                                    Err(_) => {
                                        config_candidates.push(Err(()));
                                        continue
                                    }
                                }
                            }
                            Err(_) => {
                                config_candidates.push(Err(()));
                                continue
                            }
                        }
                    }
                    Err(_) => {
                        config_candidates.push(Err(()));
                        continue
                    }
                }
            }
            for candidate in config_candidates {
                if let Ok(c) = candidate {
                    return c
                }
            }
            return Config::default();
        }
    }
}
impl Default for Config {
    fn default() -> Self {
        Self { 
            detector_interface_name: "ens19".to_owned(), 
            server_port: 5001_u16 
        }
    }
}
