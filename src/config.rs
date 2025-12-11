use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub servers: HashMap<String, HostConfig>,
    pub client: Option<ClientDefaults>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct HostConfig {
    pub listen: String,
    pub token_file: PathBuf,
    pub worlds: Vec<PathBuf>,
    pub schematics: Vec<PathBuf>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ClientDefaults {
    pub default_server: Option<String>,
    pub token_file: Option<PathBuf>,
    pub output_dir: Option<PathBuf>,
}

impl AppConfig {
    pub fn load(path: Option<&Path>) -> Result<Self> {
        let exe_dir = std::env::current_exe()
            .ok()
            .and_then(|p| p.parent().map(|p| p.to_path_buf()));
        let cwd_path = PathBuf::from("k2.yaml");
        let exe_path = exe_dir.as_ref().map(|p| p.join("k2.yaml"));

        let resolved = if let Some(p) = path {
            p.to_path_buf()
        } else if cwd_path.exists() {
            cwd_path
        } else if let Some(p) = exe_path {
            p
        } else {
            cwd_path
        };

        let contents = fs::read_to_string(&resolved)
            .with_context(|| format!("reading config from {}", resolved.display()))?;
        let mut cfg: AppConfig =
            serde_yaml::from_str(&contents).context("parsing YAML config into AppConfig")?;
        if cfg.servers.is_empty() {
            return Err(anyhow!("config must include at least one server entry"));
        }
        let base_dir = resolved.parent().unwrap_or_else(|| Path::new("."));
        normalize_paths(&mut cfg, base_dir);
        Ok(cfg)
    }
}

fn normalize_paths(cfg: &mut AppConfig, base: &Path) {
    for (_, host) in cfg.servers.iter_mut() {
        host.token_file = resolve_relative(host.token_file.clone(), base);
        host.worlds = host
            .worlds
            .iter()
            .cloned()
            .map(|p| resolve_relative(p, base))
            .collect();
        host.schematics = host
            .schematics
            .iter()
            .cloned()
            .map(|p| resolve_relative(p, base))
            .collect();
    }
    if let Some(client) = cfg.client.as_mut() {
        if let Some(ref mut token) = client.token_file {
            *token = resolve_relative(token.clone(), base);
        }
        if let Some(ref mut out) = client.output_dir {
            *out = resolve_relative(out.clone(), base);
        }
    }
}

fn resolve_relative(path: PathBuf, base: &Path) -> PathBuf {
    if path.is_absolute() {
        path
    } else {
        base.join(path)
    }
}
