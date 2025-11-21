use crate::errors::{ApitapError, Result};
use crate::utils::datafusion_ext::{
    get_shared_context, DataFrameExt, JsonStreamType, JsonValueExt, QueryResultStream,
};
use crate::utils::schema::infer_schema_from_values;
use crate::utils::table_provider::JsonStreamTableProvider;
use crate::utils::{http_retry, schema};
use crate::writer::{DataWriter, WriteMode};
use async_trait::async_trait;
use datafusion::arrow;
use datafusion::arrow::datatypes::SchemaRef;
use futures::stream::{self, BoxStream, StreamExt, TryStreamExt};
use futures::Stream;
use reqwest::header::CONTENT_TYPE;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::{
    codec::{FramedRead, LinesCodec},
    io::StreamReader,
};
use tracing::{debug, debug_span, error, info, info_span, trace, warn};

// =========================== NDJSON helper ===================================
pub type BoxStreamCustom<T> = Pin<Box<dyn Stream<Item = T> + Send + 'static>>;

/// Stream an HTTP response as NDJSON and flatten an optional JSON pointer (`/data`, etc.).
/// If `data_path` is None, it will try to flatten the top-level array; otherwise it yields the object.
pub async fn ndjson_stream_qs(
    client: &reqwest::Client,
    url: &str,
    query: &[(String, String)],
    data_path: Option<&str>,
    config_retry: &crate::pipeline::Retry,
) -> Result<BoxStream<'static, Result<Value>>> {
    // Instrument HTTP/NDJSON parsing for tracing with source and optional data_path
    let span = info_span!("http.ndjson_stream", source = %url, query_len = query.len());
    let _g = span.enter();
    let client_with_retry = http_retry::build_client_with_retry(client.clone(), config_retry);

    // Instrument the HTTP request/response at debug level with timing and status
    let req_span =
        debug_span!("http.request", method = "GET", source = %url, query_len = query.len());
    let _req_g = req_span.enter();
    let started = std::time::Instant::now();

    let resp = client_with_retry.get(url).query(query).send().await?;

    let status = resp.status();
    let elapsed = started.elapsed();
    debug!(status = %status, elapsed_ms = elapsed.as_millis(), "http response received");

    let resp = resp.error_for_status()?;

    // Heuristic: treat as NDJSON only if content-type says so
    let is_ndjson = resp
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|h| h.to_str().ok())
        .map(|ct| ct.contains("ndjson") || ct.contains("x-ndjson"))
        .unwrap_or(false);

    if !is_ndjson {
        // -------- Regular JSON (object or array) path --------
        let bytes = resp.bytes().await?;
        let v: Value = serde_json::from_slice(&bytes)?;

        // If data_path is provided, drill into it; else use the whole value.
        let target = if let Some(p) = data_path {
            v.pointer(p).cloned().unwrap_or(Value::Null)
        } else {
            v
        };

        let items: Vec<Value> = if let Some(arr) = target.as_array() {
            arr.clone()
        } else if target.is_null() {
            Vec::new()
        } else {
            vec![target]
        };

        debug!(items = items.len(), "parsed JSON response items");

        // Emit as a stream of Values
        let st = stream::iter(items.into_iter().map(Ok)).boxed();
        return Ok(st);
    }

    // -------- NDJSON path (one JSON per line) --------
    let byte_stream = resp
        .bytes_stream()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));

    let reader = StreamReader::new(byte_stream);
    let lines = FramedRead::new(reader, LinesCodec::new());
    let data_path_owned = data_path.map(|s| s.to_owned());

    let s = async_stream::try_stream! {
        let mut lines = lines;
        while let Some(line_res) = lines.next().await {
            let line = line_res?;
            let trimmed = line.trim();
            if trimmed.is_empty() { continue; }

            trace!(len = trimmed.len(), "ndjson line");

            let v: Value = serde_json::from_str(trimmed)?;

            if let Some(ref p) = data_path_owned {
                if let Some(inner) = v.pointer(p) {
                    if let Some(arr) = inner.as_array() {
                        for item in arr { yield item.clone(); }
                    } else if !inner.is_null() {
                        yield inner.clone();
                    }
                    continue;
                }
            }

            if let Some(arr) = v.as_array() {
                for item in arr { yield item.clone(); }
            } else {
                yield v;
            }
        }
    };
    Ok(s.boxed())
}

// =============================== Page Writer =================================

#[async_trait]
pub trait PageWriter: Send + Sync {
    async fn write_page(
        &self,
        _page_number: u64,
        _data: Vec<Value>,
        _write_mode: WriteMode,
    ) -> Result<()>;

    async fn write_page_stream(
        &self,
        _stream_data: Pin<Box<dyn Stream<Item = Result<Value>> + Send>>,
        _write_mode: WriteMode,
    ) -> Result<()> {
        Ok(())
    }

    async fn on_page_error(&self, page_number: u64, error: String) -> Result<()> {
        error!(page = page_number, %error, "error fetching page");
        Ok(())
    }

    async fn begin(&self) -> Result<()> {
        Ok(())
    }
    async fn commit(&self) -> Result<()> {
        Ok(())
    }
}

// =========================== Pagination types ================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Pagination {
    LimitOffset {
        limit_param: String,
        offset_param: String,
    },
    PageNumber {
        page_param: String,
        per_page_param: String,
    },
    PageOnly {
        page_param: String,
    },
    Cursor {
        cursor_param: String,
        page_size_param: Option<String>,
    },
    Default,
}

/// Hint to compute total pages.
/// - Items: pointer points to total items; pages = ceil(items/limit)
/// - Pages:  pointer points directly to total pages
#[derive(Debug, Clone)]
pub enum TotalHint {
    Items { pointer: String },
    Pages { pointer: String },
}

// =========================== Fetcher =========================================

pub struct PaginatedFetcher {
    client: Client,
    base_url: String,
    concurrency: usize,
    pagination_config: Pagination,
    batch_size: usize,
}

impl PaginatedFetcher {
    pub fn new(client: Client, base_url: impl Into<String>, concurrency: usize) -> Self {
        Self {
            client,
            base_url: base_url.into(),
            concurrency,
            pagination_config: Pagination::Default,
            batch_size: 256,
        }
    }

    pub fn with_limit_offset(
        mut self,
        limit_param: impl Into<String>,
        offset_param: impl Into<String>,
    ) -> Self {
        self.pagination_config = Pagination::LimitOffset {
            limit_param: limit_param.into(),
            offset_param: offset_param.into(),
        };
        self
    }

    pub fn with_page_number(
        mut self,
        page_param: impl Into<String>,
        per_page_param: impl Into<String>,
    ) -> Self {
        self.pagination_config = Pagination::PageNumber {
            page_param: page_param.into(),
            per_page_param: per_page_param.into(),
        };
        self
    }

    pub fn with_batch_size(mut self, n: usize) -> Self {
        self.batch_size = n.max(1);
        self
    }

    pub async fn limit_offset_stream(
        &self,
        limit: u64,
        data_path: Option<&str>,
        extra_params: Option<&[(String, String)]>,
        config_retry: &crate::pipeline::Retry,
    ) -> crate::errors::Result<JsonStreamType> {
        let (limit_param, offset_param) = match &self.pagination_config {
            Pagination::LimitOffset {
                limit_param,
                offset_param,
            } => (limit_param.clone(), offset_param.clone()),
            other => {
                return Err(crate::errors::ApitapError::PaginationError(format!(
                    "Pagination::LimitOffset not configured {other:?}"
                )));
            }
        };

        let client = self.client.clone();
        let base_url = self.base_url.clone();
        let data_path_owned = data_path.map(|s| s.to_string());
        let retry_cfg = config_retry.clone();
        let extra_params_owned = extra_params.map(|p| p.to_vec()).unwrap_or_default();

        // Build the stream
        let s = async_stream::try_stream! {
            let mut offset: u64 = 0;

            loop {
                // Merge pagination params with extra params
                let mut query_params = extra_params_owned.clone();
                query_params.push((limit_param.clone(), limit.to_string()));
                query_params.push((offset_param.clone(), offset.to_string()));

                let mut page_stream: BoxStream<'static, crate::errors::Result<Value>> =
                    ndjson_stream_qs(
                        &client,
                        &base_url,
                        &query_params,
                        data_path_owned.as_deref(),
                        &retry_cfg,
                    ).await?;

                let mut page_count = 0usize;

                while let Some(item) = page_stream.next().await {
                    let v = item?;
                    page_count += 1;
                    yield v;
                }

                if page_count == 0 {
                    break;
                }

                offset += limit;
            }
        };

        Ok(Box::pin(s))
    }

    /// LIMIT/OFFSET mode. If `total_hint` is None, it fetches until a page yields 0 rows.
    #[allow(clippy::too_many_arguments)]
    pub async fn fetch_limit_offset(
        &self,
        limit: u64,
        data_path: Option<String>,
        extra_params: Option<&[(String, String)]>,
        _total_hint: Option<TotalHint>,
        writer: Arc<dyn PageWriter>,
        write_mode: WriteMode,
        config_retry: &crate::pipeline::Retry,
    ) -> Result<FetchStats> {
        let span = info_span!("fetch.limit_offset.stream", source = %self.base_url, limit = limit);
        let _g = span.enter();

        let mut stats = FetchStats::new();

        // Build a single JsonStreamType over all pages
        let json_stream = self
            .limit_offset_stream(limit, data_path.as_deref(), extra_params, config_retry)
            .await?;

        // Now you can wrap it into your QueryResultStream abstraction

        self.write_streamed_page(1, json_stream, &*writer, &mut stats, write_mode.clone())
            .await?;

        // You don't have per-page stats here easily, but you could compute total_items
        // inside write_stream, or wrap the stream to count rows.
        Ok(stats)
    }

    /// PAGE/PER_PAGE mode.
    pub async fn fetch_page_number(
        &self,
        per_page: u64,
        data_path: Option<&str>,
        total_hint: Option<TotalHint>,
        writer: Arc<dyn PageWriter>,
        write_mode: WriteMode,
        config_retry: &crate::pipeline::Retry,
    ) -> Result<FetchStats> {
        let (page_param, per_page_param) = match &self.pagination_config {
            Pagination::PageNumber {
                page_param,
                per_page_param,
            } => (page_param.clone(), per_page_param.clone()),
            other => {
                return Err(ApitapError::PaginationError(format!(
                    "expected Pagination::PageNumber, got {other:?}"
                )));
            }
        };

        let span = info_span!("fetch.page_number", source = %self.base_url, per_page = per_page);
        let _g = span.enter();

        writer.begin().await?;

        // First request as JSON (page=1)
        let first_json: Value = self
            .client
            .get(&self.base_url)
            .query(&[(page_param.as_str(), "1".to_string())])
            .query(&[(per_page_param.as_str(), per_page.to_string())])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        let mut stats = FetchStats::new();

        // Write page 1
        let mut wrote_first = false;
        if let Some(p) = data_path {
            if let Some(arr) = first_json.pointer(p).and_then(|v| v.as_array()).cloned() {
                let n = arr.len();
                writer.write_page(1, arr, write_mode.clone()).await?;
                stats.add_page(1, n);
                wrote_first = true;
            }
        }
        if !wrote_first {
            let s = ndjson_stream_qs(
                &self.client,
                &self.base_url,
                &[
                    (page_param.clone(), "1".into()),
                    (per_page_param.clone(), per_page.to_string()),
                ],
                data_path,
                config_retry,
            )
            .await?;
            self.write_streamed_page(1, s, &*writer, &mut stats, write_mode.clone())
                .await?;
        }

        // Determine total pages
        let pages_opt = match total_hint {
            Some(TotalHint::Items { ref pointer }) => first_json
                .pointer(pointer)
                .and_then(|v| v.as_u64())
                .map(|total_items| (total_items + per_page - 1) / per_page),
            Some(TotalHint::Pages { ref pointer }) => {
                first_json.pointer(pointer).and_then(|v| v.as_u64())
            }
            None => None,
        };

        if let Some(total_pages) = pages_opt {
            // pages 2..=total_pages
            let client = self.client.clone();
            let url = self.base_url.clone();
            let page_param_c = page_param.clone();
            let per_page_param_c = per_page_param.clone();
            let data_path_c = data_path.map(|s| s.to_string());
            let writer_ref = Arc::clone(&writer);
            let batch_size = self.batch_size;
            let write_mode_clone = write_mode.clone();

            stream::iter(2..=total_pages)
                .map(move |page| {
                    let client = client.clone();
                    let url = url.clone();
                    let page_param = page_param_c.clone();
                    let per_page_param = per_page_param_c.clone();
                    let data_path = data_path_c.clone();
                    let writer = Arc::clone(&writer_ref);
                    let write_mode_c = write_mode_clone.clone();

                    async move {
                        let mut s = match ndjson_stream_qs(
                            &client,
                            &url,
                            &[
                                (page_param, page.to_string()),
                                (per_page_param, per_page.to_string()),
                            ],
                            data_path.as_deref(),
                            config_retry,
                        )
                        .await
                        {
                            Ok(s) => s,
                            Err(e) => {
                                let _ = writer.on_page_error(page, e.to_string()).await;
                                return;
                            }
                        };
                        let mut buf = Vec::with_capacity(batch_size);
                        while let Some(item) = s.next().await {
                            match item {
                                Ok(v) => {
                                    buf.push(v);
                                    if buf.len() == batch_size {
                                        let out = std::mem::take(&mut buf);
                                        if let Err(e) =
                                            writer.write_page(page, out, write_mode_c.clone()).await
                                        {
                                            let _ = writer.on_page_error(page, e.to_string()).await;
                                        }
                                        trace!(page = page, batch = true, "wrote batch for page");
                                    }
                                }
                                Err(e) => {
                                    let _ = writer.on_page_error(page, e.to_string()).await;
                                }
                            }
                        }
                        if !buf.is_empty() {
                            let out = std::mem::take(&mut buf);
                            let cnt = out.len();
                            if let Err(e) = writer.write_page(page, out, write_mode_c.clone()).await
                            {
                                let _ = writer.on_page_error(page, e.to_string()).await;
                            } else {
                                info!(page = page, items = cnt, source = %url, "wrote page remainder");
                            }
                        }
                    }
                })
                .buffer_unordered(self.concurrency)
                .collect::<Vec<_>>()
                .await;
        } else {
            // Unknown total pages: fetch page=2,3,... until empty
            let mut page = 2u64;
            loop {
                let s = match ndjson_stream_qs(
                    &self.client,
                    &self.base_url,
                    &[
                        (page_param.clone(), page.to_string()),
                        (per_page_param.clone(), per_page.to_string()),
                    ],
                    data_path,
                    config_retry,
                )
                .await
                {
                    Ok(s) => s,
                    Err(e) => {
                        let _ = writer.on_page_error(page, e.to_string()).await;
                        break;
                    }
                };

                let wrote = self
                    .write_streamed_page(page, s, &*writer, &mut stats, write_mode.clone())
                    .await?;
                if wrote == 0 {
                    break;
                } // stop on empty page
                page += 1;
            }
        }

        writer.commit().await?;
        Ok(stats)
    }

    // -------------------- Private helpers ------------------------------------

    async fn write_streamed_page(
        &self,
        _page: u64,
        s: BoxStreamCustom<Result<Value>>,
        writer: &dyn PageWriter,
        stats: &mut FetchStats,
        write_mode: WriteMode,
    ) -> Result<usize> {
        // Use atomic counter instead of Mutex for better performance
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = Arc::clone(&count);

        let counted_stream = s.map(move |result| {
            if result.is_ok() {
                count_clone.fetch_add(1, Ordering::Relaxed);
            }
            result
        });

        writer
            .write_page_stream(Box::pin(counted_stream), write_mode)
            .await?;

        // Get final count
        let final_count = count.load(Ordering::Relaxed);
        stats.add_page(_page, final_count);
        Ok(final_count)
    }
}

// ============================== Stats =======================================

#[derive(Debug, Clone)]
pub struct FetchStats {
    pub success_count: usize,
    pub error_count: usize,
    pub total_items: usize,
}

impl Default for FetchStats {
    fn default() -> Self {
        Self::new()
    }
}

impl FetchStats {
    pub fn new() -> Self {
        Self {
            success_count: 0,
            error_count: 0,
            total_items: 0,
        }
    }
    fn add_page(&mut self, _page: u64, items: usize) {
        self.success_count += 1;
        self.total_items += items;
    }
    #[allow(dead_code)]
    fn add_error(&mut self, _page: u64) {
        self.error_count += 1;
    }
}

// ===================== Example Writers (unchanged in spirit) =================

pub struct DataFusionPageWriter {
    table_name: String,
    sql: String,
    final_writer: Arc<dyn DataWriter>,
}
impl DataFusionPageWriter {
    pub fn new(
        table_name: impl Into<String>,
        sql: impl Into<String>,
        final_writer: Arc<dyn DataWriter>,
    ) -> Self {
        Self {
            table_name: table_name.into(),
            sql: sql.into(),
            final_writer,
        }
    }
}

#[async_trait]
impl PageWriter for DataFusionPageWriter {
    async fn write_page(
        &self,
        page_number: u64,
        data: Vec<Value>,
        write_mode: WriteMode,
    ) -> Result<()> {
        // Span covering transform -> write for this page
        let items = data.len();
        let span = info_span!("transform.load", table = %self.table_name, page = page_number, items = items);
        let _g = span.enter();

        let json_array = Value::Array(data);
        let sdf = json_array.to_sql(&self.table_name, &self.sql).await?;
        let result_stream = sdf.inner().to_stream().await?;
        // Use structured fields for the downstream writer call
        let table_page = format!("{}_page_{}", self.table_name, page_number);
        self.final_writer
            .write_stream(
                QueryResultStream {
                    table_name: table_page,
                    data: result_stream,
                },
                write_mode,
            )
            .await?;
        Ok(())
    }

    async fn write_page_stream(
        &self,
        json_stream: Pin<Box<dyn Stream<Item = Result<serde_json::Value>> + Send>>,
        _write_mode: WriteMode,
    ) -> Result<()> {
        debug!("starting streaming pipeline");
        let ctx = get_shared_context().await;

        // Single-producer, single-consumer channel with increased buffer for better throughput
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Result<serde_json::Value>>(8192);

        // Move the ONLY sender into the task so the channel closes when done.
        let _stream_task = tokio::spawn(async move {
            let mut pinned = json_stream;
            while let Some(item) = pinned.next().await {
                // If the receiver is gone, stop.
                if tx.send(item).await.is_err() {
                    break;
                }
            }
            // tx dropped here -> rx will eventually return None
        });

        // --------- Sample for schema (and keep the samples) ---------
        let mut samples: Vec<serde_json::Value> = Vec::new();

        while samples.len() < 100 {
            match rx.recv().await {
                Some(Ok(v)) => samples.push(v),
                Some(Err(e)) => {
                    // propagate upstream error
                    return Err(e);
                }
                None => break, // producer finished
            }
        }

        if samples.is_empty() {
            info!("Stream empty. Exit");
            return Ok(());
        }

        let arrow_schema = infer_schema_from_values(&samples)?;
        debug!(
            fields = arrow_schema.fields().len(),
            field_names = ?arrow_schema.fields().iter().map(|f| f.name().as_str()).collect::<Vec<_>>(),
            "inferred schema"
        );

        // Put samples in a shared queue so the provider's stream can emit them first.
        let prefix = Arc::new(Mutex::new(VecDeque::from(samples)));

        // Wrap remaining receiver so provider can pull from it.
        let rx_arc = Arc::new(Mutex::new(rx));

        // Stream factory: drain `prefix` first, then read from mpsc::Receiver.
        let stream_factory = {
            let prefix = Arc::clone(&prefix);
            let rx_arc = Arc::clone(&rx_arc);
            move || {
                let prefix = Arc::clone(&prefix);
                let rx_arc = Arc::clone(&rx_arc);
                async_stream::stream! {
                    loop {
                        // 1) yield buffered samples first
                        if let Some(v) = { prefix.lock().await.pop_front() } {
                            yield Ok(v);
                            continue;
                        }

                        // 2) then read from the channel
                        let mut r = rx_arc.lock().await;
                        match r.recv().await {
                            Some(item) => {
                                drop(r); // release lock before yield
                                yield item;
                            }
                            None => break, // channel closed: end of stream
                        }
                    }
                }
                .boxed()
            }
        };

        // Create table provider with schema
        let table_provider = JsonStreamTableProvider::new(Arc::new(stream_factory), arrow_schema);

        // Use a unique table name to avoid conflicts in shared context
        // Use only alphanumeric characters to avoid SQL parsing issues
        let alphabet: [char; 36] = [
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g',
            'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x',
            'y', 'z',
        ];
        let unique_id = nanoid::nanoid!(10, &alphabet);
        let unique_table_name = format!("{}_{}", self.table_name, unique_id);

        // Deregister table if it exists from previous runs (best effort)
        let _ = ctx.deregister_table(&unique_table_name);

        ctx.register_table(unique_table_name.clone(), Arc::new(table_provider))?;

        // Replace the original table name in SQL with the unique table name
        let sql_with_unique_table = self.sql.replace(&self.table_name, &unique_table_name);

        let df = ctx.sql(&sql_with_unique_table).await?;

        // Execute query and get streaming results
        let record_batch_stream = df.execute_stream().await?;

        // Convert RecordBatch stream to JSON stream for the writer
        let json_value_stream = convert_record_batch_to_json(record_batch_stream);

        // Write the streaming results to the final destination
        self.final_writer
            .write_stream(
                QueryResultStream {
                    table_name: self.table_name.clone(),
                    data: json_value_stream,
                },
                _write_mode,
            )
            .await?;

        // Clean up: deregister the table
        let _ = ctx.deregister_table(&unique_table_name);

        Ok(())
    }
    async fn commit(&self) -> Result<()> {
        self.final_writer.commit().await
    }
}

pub async fn infer_schema_and_create_factory(
    mut json_stream: Pin<Box<dyn Stream<Item = Result<Value>> + Send>>,
) -> Result<(
    SchemaRef,
    Arc<dyn Fn() -> std::pin::Pin<Box<dyn Stream<Item = Result<Value>> + Send>> + Send + Sync>,
)> {
    let mut items = Vec::new();
    let mut sample_count = 0;
    const SAMPLE_SIZE: usize = 100; // Sample first 100 items

    // Step 1: Collect sample for schema inference
    while let Some(item) = json_stream.next().await {
        items.push(item?);
        sample_count += 1;
        if sample_count >= SAMPLE_SIZE {
            break;
        }
    }

    // Step 2: Infer schema from sample
    let schema = schema::infer_schema_from_values(&items)?;

    // Step 3: Continue collecting rest of items
    while let Some(item) = json_stream.next().await {
        items.push(item?);
    }

    // Step 4: Create factory
    let factory = Arc::new(move || {
        let data = items.clone();
        Box::pin(futures::stream::iter(data.into_iter().map(Ok)))
            as std::pin::Pin<Box<dyn Stream<Item = Result<Value>> + Send>>
    });

    Ok((schema, factory))
}

#[allow(dead_code)]
fn convert_record_batch_to_json(
    mut stream: datafusion::execution::SendableRecordBatchStream,
) -> std::pin::Pin<Box<dyn Stream<Item = Result<serde_json::Value>> + Send + 'static>> {
    let json_stream = async_stream::try_stream! {
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result
                .map_err(|e| ApitapError::PipelineError(format!("RecordBatch error: {}", e)))?;

            for row_index in 0..batch.num_rows() {
                let mut row_json = serde_json::Map::new();

                for (col_index, field) in batch.schema().fields().iter().enumerate() {
                    let column = batch.column(col_index);
                    let value = arrow_value_to_json(column, row_index)
                        .unwrap_or(serde_json::Value::Null);
                    row_json.insert(field.name().clone(), value);
                }

                yield serde_json::Value::Object(row_json);
            }
        }
    };

    Box::pin(json_stream)
}
/// Convert a single Arrow array value to JSON
#[allow(dead_code)]
fn arrow_value_to_json(
    column: &arrow::array::ArrayRef,
    row_index: usize,
) -> Result<serde_json::Value> {
    use arrow::array::*;

    if column.is_null(row_index) {
        return Ok(serde_json::Value::Null);
    }

    match column.data_type() {
        arrow::datatypes::DataType::Null => Ok(serde_json::Value::Null),
        arrow::datatypes::DataType::Boolean => {
            let array = column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| ApitapError::DataTypeError("Expected BooleanArray".to_string()))?;
            Ok(serde_json::Value::Bool(array.value(row_index)))
        }
        arrow::datatypes::DataType::Int64 => {
            let array = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| ApitapError::DataTypeError("Expected Int64Array".to_string()))?;
            Ok(serde_json::json!(array.value(row_index)))
        }
        arrow::datatypes::DataType::Int32 => {
            let array = column
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| ApitapError::DataTypeError("Expected Int32Array".to_string()))?;
            Ok(serde_json::json!(array.value(row_index) as i64))
        }
        arrow::datatypes::DataType::UInt64 => {
            let array = column
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| ApitapError::DataTypeError("Expected UInt64Array".to_string()))?;
            Ok(serde_json::json!(array.value(row_index)))
        }
        arrow::datatypes::DataType::UInt32 => {
            let array = column
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| ApitapError::DataTypeError("Expected UInt32Array".to_string()))?;
            Ok(serde_json::json!(array.value(row_index) as u64))
        }
        arrow::datatypes::DataType::Float64 => {
            let array = column
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| ApitapError::DataTypeError("Expected Float64Array".to_string()))?;
            Ok(serde_json::json!(array.value(row_index)))
        }
        arrow::datatypes::DataType::Float32 => {
            let array = column
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| ApitapError::DataTypeError("Expected Float32Array".to_string()))?;
            Ok(serde_json::json!(array.value(row_index) as f64))
        }
        arrow::datatypes::DataType::Utf8 => {
            let array = column
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| ApitapError::DataTypeError("Expected StringArray".to_string()))?;
            Ok(serde_json::Value::String(
                array.value(row_index).to_string(),
            ))
        }
        arrow::datatypes::DataType::LargeUtf8 => {
            let array = column
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| {
                    ApitapError::DataTypeError("Expected LargeStringArray".to_string())
                })?;
            Ok(serde_json::Value::String(
                array.value(row_index).to_string(),
            ))
        }
        arrow::datatypes::DataType::Utf8View => {
            let array = column
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| {
                    ApitapError::DataTypeError("Expected StringViewArray".to_string())
                })?;
            Ok(serde_json::Value::String(
                array.value(row_index).to_string(),
            ))
        }
        arrow::datatypes::DataType::Struct(fields) => {
            let array = column
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| ApitapError::DataTypeError("Expected StructArray".to_string()))?;
            let mut obj = serde_json::Map::new();

            for (i, field) in fields.iter().enumerate() {
                let col = array.column(i);
                let value = arrow_value_to_json(col, row_index)?;
                obj.insert(field.name().clone(), value);
            }

            Ok(serde_json::Value::Object(obj))
        }
        arrow::datatypes::DataType::List(_field) => {
            let array = column
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| ApitapError::DataTypeError("Expected ListArray".to_string()))?;
            let list = array.value(row_index);
            let mut arr = Vec::new();

            for i in 0..list.len() {
                arr.push(arrow_value_to_json(&list, i)?);
            }

            Ok(serde_json::Value::Array(arr))
        }
        arrow::datatypes::DataType::LargeList(_field) => {
            let array = column
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| ApitapError::DataTypeError("Expected LargeListArray".to_string()))?;
            let list = array.value(row_index);
            let mut arr = Vec::new();

            for i in 0..list.len() {
                arr.push(arrow_value_to_json(&list, i)?);
            }

            Ok(serde_json::Value::Array(arr))
        }
        other => {
            warn!("Unsupported Arrow type: {:?}, converting to null", other);
            Ok(serde_json::Value::Null)
        }
    }
}
