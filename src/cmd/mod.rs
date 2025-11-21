//! Command-line interface and pipeline execution module.
//!
//! This module provides the CLI structure and the main pipeline execution logic
//! for extracting data from REST APIs, transforming it with SQL, and loading it
//! into data warehouses.

use std::sync::{Arc, Mutex};
use std::time::Instant;

use clap::Parser;
use tracing::{debug, info, instrument, warn};

use crate::config::load_config_from_path;
use crate::config::templating::{
    build_env_with_captures, list_sql_templates, render_one, RenderCapture,
};
use crate::errors::{self, Result};
use crate::http::Http;
use crate::pipeline::run::{run_fetch, FetchOpts};
use crate::pipeline::sink::{MakeWriter, WriterOpts};
use crate::pipeline::Config;
use crate::pipeline::SinkConn;
use crate::pipeline::Source;
use crate::writer::WriteMode;

/// Default number of concurrent requests for fetching data.
const CONCURRENCY: usize = 5;

/// Default page size for paginated API requests.
const DEFAULT_PAGE_SIZE: usize = 50;

/// Batch size for fetching records.
const FETCH_BATCH_SIZE: usize = 256;

/// Command-line interface structure for the Apitap ETL tool.
#[derive(Parser, Debug)]
#[command(
    name = "apitap-run",
    version,
    about = "Extract from REST APIs, transform with SQL, load to warehouses.",
    long_about = "Extract from REST APIs, transform with SQL, load to warehouses.\n\
HTTP-to-warehouse ETL powered by DataFusion.\n\n\
Resources:\n  • Modules: Jinja-like SQL templates that declare {{ sink(...) }} and {{ use_source(...) }}\n  • YAML config: defines sources (HTTP + pagination) and targets (warehouses)\n  • Execution: fetch JSON → DataFusion SQL → write via sink-specific writers"
)]
pub struct Cli {
    /// Directory containing SQL module templates.
    #[arg(
        long = "modules",
        short = 'm',
        value_name = "DIR",
        default_value = "pipelines"
    )]
    pub modules: String,

    /// Path to the YAML configuration file.
    #[arg(
        long = "yaml-config",
        short = 'y',
        value_name = "FILE",
        default_value = "pipelines.yaml"
    )]
    pub yaml_config: String,

    /// Emit logs in JSON format.
    #[arg(long = "log-json")]
    pub log_json: bool,

    /// Set log level (overrides env vars like RUST_LOG).
    ///
    /// Example: info,warn,debug
    #[arg(long = "log-level")]
    pub log_level: Option<String>,
}

/// Main pipeline execution function.
///
/// This function orchestrates the entire ETL pipeline:
/// 1. Discovers and loads SQL templates
/// 2. Loads configuration from YAML
/// 3. Processes each template to extract, transform, and load data
///
/// # Arguments
///
/// * `root` - Root directory containing SQL templates
/// * `cfg_path` - Path to the YAML configuration file
///
/// # Errors
///
/// Returns an error if:
/// - Template discovery fails
/// - Configuration loading fails
/// - Source or target resolution fails
/// - HTTP client creation fails
/// - Data fetching or writing fails
#[instrument(
    name = "run_pipeline",
    err,
    skip_all, // Don't record large args by default
)]
pub async fn run_pipeline(root: &str, cfg_path: &str) -> Result<()> {
    log_pipeline_start();

    let start_time = Instant::now();

    // Discover SQL templates and load configuration
    let template_names = list_sql_templates(root)?;
    info!("📂 Discovered {} SQL module(s)", template_names.len());

    let config = load_config_from_path(cfg_path)?;
    info!("⚙️  Configuration loaded successfully");

    // Initialize templating environment
    let capture = Arc::new(Mutex::new(RenderCapture::default()));
    let env = build_env_with_captures(root, &capture);

    // Configure fetch options
    let fetch_opts = create_fetch_options();
    debug!(?fetch_opts, "Fetch options configured");

    // Process each template
    for (index, name) in template_names.into_iter().enumerate() {
        process_template(index + 1, name, &env, &capture, &config, &fetch_opts).await?;
    }

    log_pipeline_complete(start_time.elapsed().as_millis());
    Ok(())
}

/// Creates fetch options with default values.
fn create_fetch_options() -> FetchOpts {
    FetchOpts {
        concurrency: CONCURRENCY,
        default_page_size: DEFAULT_PAGE_SIZE,
        fetch_batch_size: FETCH_BATCH_SIZE,
    }
}

/// Processes a single SQL template through the ETL pipeline.
#[allow(clippy::too_many_arguments)]
async fn process_template(
    index: usize,
    name: String,
    env: &minijinja::Environment<'_>,
    capture: &Arc<Mutex<RenderCapture>>,
    config: &Config,
    fetch_opts: &FetchOpts,
) -> Result<()> {
    let span = tracing::info_span!("module", idx = index, name = %name);
    let _guard = span.enter();

    // Render template and extract metadata
    let rendered = render_one(env, capture, &name)?;
    let source_name = &rendered.capture.source;
    let sink_name = &rendered.capture.sink;

    // Resolve source and target configurations
    let source = config
        .source(source_name)
        .ok_or_else(|| create_config_error("source", source_name))?;

    let target = config
        .target(sink_name)
        .ok_or_else(|| create_config_error("target", sink_name))?;

    // Build HTTP client with configured headers
    let client = build_http_client(source)?;
    let url = reqwest::Url::parse(&Http::new(source.url.clone()).get_url())?;

    // Prepare destination table and SQL
    let dest_table = extract_destination_table(source, source_name)?;
    let sql = rendered.sql.replace(source_name, dest_table);

    // Initialize writer with configuration
    let writer_opts = create_writer_options(dest_table, source);
    debug!(?writer_opts, "Writer options configured");

    let connection = target.create_conn().await?;
    let (writer, maybe_truncate) = connection.make_writer(&writer_opts)?;

    // Execute truncate hook if provided
    if let Some(truncate_hook) = maybe_truncate {
        truncate_hook().await?;
    }

    // Execute ETL pipeline
    log_module_start(&name, source_name, dest_table);
    let module_start = Instant::now();

    let stats = run_fetch(
        client,
        url,
        source.data_path.clone(),
        source.query_params.clone(),
        &source.pagination,
        &sql,
        dest_table,
        writer,
        writer_opts.write_mode,
        fetch_opts,
        &source.retry,
    )
    .await?;

    log_module_complete(stats.total_items, module_start.elapsed().as_millis());
    Ok(())
}

/// Builds an HTTP client with configured headers from the source.
fn build_http_client(source: &Source) -> Result<reqwest::Client> {
    let mut http = Http::new(source.url.clone());

    if let Some(headers) = &source.headers {
        for header in headers {
            http = http.header(&header.key, &header.value);
        }
    }

    Ok(http.build_client())
}

/// Extracts the destination table name from the source configuration.
fn extract_destination_table<'a>(source: &'a Source, source_name: &str) -> Result<&'a str> {
    source.table_destination_name.as_deref().ok_or_else(|| {
        warn!(%source_name, "Missing table_destination_name");
        errors::ApitapError::PipelineError(format!(
            "table_destination_name is required for source: {source_name}"
        ))
    })
}

/// Creates writer options with sensible defaults.
fn create_writer_options<'a>(dest_table: &'a str, source: &Source) -> WriterOpts<'a> {
    WriterOpts {
        dest_table,
        primary_key: source.primary_key_in_dest.clone(),
        batch_size: 50,
        sample_size: 10,
        auto_create: true,
        auto_truncate: false,
        truncate_first: false,
        write_mode: WriteMode::Merge,
    }
}

/// Creates a configuration error for missing source or target.
fn create_config_error(config_type: &str, name: &str) -> errors::ApitapError {
    errors::ApitapError::PipelineError(format!("{config_type} not found in config: {name}"))
}

// Logging helper functions

/// Logs the start of the pipeline execution.
fn log_pipeline_start() {
    info!("═══════════════════════════════════════════════════════════");
    info!("🚀 Starting Apitap Pipeline Execution");
    info!("═══════════════════════════════════════════════════════════");
}

/// Logs the completion of the pipeline execution.
fn log_pipeline_complete(duration_ms: u128) {
    info!("═══════════════════════════════════════════════════════════");
    info!("🎉 All Pipelines Completed Successfully!");
    info!("⏱️  Total Execution Time: {duration_ms}ms");
    info!("═══════════════════════════════════════════════════════════");
}

/// Logs the start of processing a module.
fn log_module_start(name: &str, source_name: &str, dest_table: &str) {
    info!("───────────────────────────────────────────────────────────");
    info!("📋 Module: {name} | Source: {source_name} → Table: {dest_table}");
    info!("🔄 Starting ETL Pipeline...");
}

/// Logs the completion of processing a module.
fn log_module_complete(total_items: usize, duration_ms: u128) {
    info!("✅ Module Completed | Records: {total_items} | Duration: {duration_ms}ms");
}
