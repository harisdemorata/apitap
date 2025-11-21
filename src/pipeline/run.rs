use reqwest::Client;
use std::sync::Arc;
use url::Url;

use crate::http::fetcher::FetchStats;
use crate::pipeline::QueryParam;
use crate::{
    errors::{ApitapError, Result},
    http::fetcher::{DataFusionPageWriter, PaginatedFetcher, Pagination},
    writer::{DataWriter, WriteMode},
};

#[derive(Debug, Clone)]
pub struct FetchOpts {
    pub concurrency: usize,
    pub default_page_size: usize,
    pub fetch_batch_size: usize, // internal http batch size
}

#[allow(clippy::too_many_arguments)]
pub async fn run_fetch(
    client: Client,
    url: Url,
    data_path: Option<String>,
    extra_params: Option<Vec<QueryParam>>,
    pagination: &Option<Pagination>,
    sql: &str,
    dest_table: &str,
    writer: Arc<dyn DataWriter>,
    write_mode: WriteMode,
    opts: &FetchOpts,
    config_retry: &crate::pipeline::Retry,
) -> Result<FetchStats> {
    let page_writer = Arc::new(DataFusionPageWriter::new(dest_table, sql, writer.clone()));

    // Convert QueryParam to (String, String) tuples
    let extra_params_vec: Vec<(String, String)> = extra_params
        .unwrap_or_default()
        .into_iter()
        .map(|q| (q.key, q.value))
        .collect();

    match pagination {
        Some(Pagination::LimitOffset {
            limit_param,
            offset_param,
        }) => {
            let fetcher = PaginatedFetcher::new(client, url, opts.concurrency)
                .with_limit_offset(limit_param, offset_param)
                .with_batch_size(opts.fetch_batch_size);

            let page_size: u64 = opts.default_page_size.try_into().map_err(|_| {
                ApitapError::ConfigError(format!(
                    "Invalid page size: {} (must fit in u64)",
                    opts.default_page_size
                ))
            })?;

            let stats = fetcher
                .fetch_limit_offset(
                    page_size,
                    data_path,
                    Some(&extra_params_vec),
                    None,
                    page_writer,
                    write_mode,
                    config_retry,
                )
                .await?;
            Ok(stats)
        }

        Some(Pagination::PageNumber {
            page_param,
            per_page_param,
        }) => {
            let page_writer = Arc::new(DataFusionPageWriter::new(dest_table, sql, writer.clone()));

            let fetcher = PaginatedFetcher::new(client, url, opts.concurrency)
                .with_batch_size(opts.fetch_batch_size)
                .with_page_number(page_param, per_page_param);

            let per_page: u64 = opts.default_page_size.try_into().map_err(|_| {
                ApitapError::ConfigError(format!(
                    "Invalid page size: {} (must fit in u64)",
                    opts.default_page_size
                ))
            })?;

            let stats = fetcher
                .fetch_page_number(
                    per_page,
                    data_path.as_deref(),
                    None,
                    page_writer,
                    write_mode,
                    config_retry,
                )
                .await?;

            Ok(stats)
        }

        Some(Pagination::PageOnly { page_param: _ }) => {
            let _fetcher = PaginatedFetcher::new(client, url, opts.concurrency)
                .with_batch_size(opts.fetch_batch_size);
            Ok(FetchStats::new())
        }

        Some(Pagination::Cursor {
            cursor_param: _,
            page_size_param: _,
        }) => {
            let _fetcher = PaginatedFetcher::new(client, url, opts.concurrency)
                .with_batch_size(opts.fetch_batch_size);
            Ok(FetchStats::new())
        }

        Some(Pagination::Default) | None => Err(ApitapError::PaginationError(
            "no supported pagination configured".into(),
        )),
    }
}
