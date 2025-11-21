// tracing_setup.rs
use tracing_error::ErrorLayer;
use tracing_subscriber::{fmt, layer::SubscriberExt, EnvFilter, Registry};

/// Initialize tracing subscriber.
///
/// Behavior:
/// - Log level is read from `APITAP_LOG_LEVEL` if set, otherwise falls back to `RUST_LOG` (via try_from_default_env),
///   then to `info`.
/// - Output format can be set via `APITAP_LOG_FORMAT=json` to enable JSON output. Any other value uses the default
///   human-readable formatter.
pub fn init_tracing() {
    // Read from environment for backward compatibility
    let level = std::env::var("APITAP_LOG_LEVEL").ok();
    let use_json = std::env::var("APITAP_LOG_FORMAT")
        .map(|v| v.to_lowercase() == "json")
        .unwrap_or(false);
    init_tracing_with(level.as_deref(), use_json);
}

/// Initialize tracing with explicit options.
///
/// - `level`: optional log level string (e.g., "info", "debug,crate=trace"). If `None`, falls
///   back to `RUST_LOG` or `info` as before.
/// - `use_json`: if true, enable JSON formatter.
pub fn init_tracing_with(level: Option<&str>, use_json: bool) {
    // Allow explicit level override, else fall back to RUST_LOG / default
    let filter = match level {
        Some(lvl) => EnvFilter::new(lvl),
        None => EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
    };

    if use_json {
        let subscriber = Registry::default()
            .with(filter)
            .with(
                fmt::layer()
                    .json()
                    .with_target(false)
                    .with_file(false)
                    .with_line_number(false),
            )
            .with(ErrorLayer::default());

        tracing::subscriber::set_global_default(subscriber)
            .expect("failed to set global tracing subscriber");
    } else {
        let subscriber = Registry::default()
            .with(filter)
            .with(
                fmt::layer()
                    .with_target(false)
                    .with_file(true)
                    .with_line_number(true),
            )
            .with(ErrorLayer::default());

        tracing::subscriber::set_global_default(subscriber)
            .expect("failed to set global tracing subscriber");
    }
}
