use crate::errors::Result;
use crate::pipeline::Config as PipelineConfig;
use std::env;
use std::{fs::File, path::Path};

// Validate credentials for targets that require authentication.
fn validate_credentials(cfg: &PipelineConfig) -> Result<()> {
    for tgt in &cfg.targets {
        match tgt {
            crate::pipeline::Target::Postgres(pg) => {
                let auth = &pg.auth;
                let has_inline = auth.username.is_some() && auth.password.is_some();
                let has_env_keys = auth.username_env.is_some() && auth.password_env.is_some();
                if has_inline {
                    continue;
                }
                if has_env_keys {
                    // Ensure referenced env vars exist and are non-empty
                    // Safe: checked by has_env_keys = username_env.is_some() && password_env.is_some()
                    let u_key = auth
                        .username_env
                        .as_ref()
                        .expect("username_env is Some, checked by has_env_keys guard");
                    let p_key = auth
                        .password_env
                        .as_ref()
                        .expect("password_env is Some, checked by has_env_keys guard");
                    let u_val = env::var(u_key).map_err(|_| {
                        crate::errors::ApitapError::ConfigError(format!(
                            "environment variable '{}' for postgres username not set",
                            u_key
                        ))
                    })?;
                    if u_val.trim().is_empty() {
                        return Err(crate::errors::ApitapError::ConfigError(format!(
                            "environment variable '{}' for postgres username is empty",
                            u_key
                        ))
                        .into());
                    }
                    let p_val = env::var(p_key).map_err(|_| {
                        crate::errors::ApitapError::ConfigError(format!(
                            "environment variable '{}' for postgres password not set",
                            p_key
                        ))
                    })?;
                    if p_val.trim().is_empty() {
                        return Err(crate::errors::ApitapError::ConfigError(format!(
                            "environment variable '{}' for postgres password is empty",
                            p_key
                        ))
                        .into());
                    }
                    continue;
                }
                return Err(crate::errors::ApitapError::ConfigError(format!("postgres target '{}' missing credentials; provide username/password or username_env/password_env", pg.name)).into());
            }
        }
    }
    Ok(())
}

pub mod templating;

pub fn load_config_from_path<P: AsRef<Path>>(path: P) -> Result<PipelineConfig> {
    let f = File::open(path)?;
    let cfg: PipelineConfig = serde_yaml::from_reader(f)?;
    // Validate credentials (ensures env vars referenced exist)
    validate_credentials(&cfg)?;
    Ok(cfg)
}
