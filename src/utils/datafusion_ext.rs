// src/utils/datafusion_ext.rs

use async_trait::async_trait;
use datafusion::error::DataFusionError::ArrowError as DatafusionArrowError;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::{
    arrow::{datatypes::FieldRef, error::ArrowError, record_batch::RecordBatch},
    dataframe::DataFrame,
    execution::{context::SessionConfig, memory_pool::GreedyMemoryPool},
    prelude::*,
};
use futures::{stream, Stream, StreamExt};
use serde_arrow::schema::{SchemaLike, TracingOptions};
use std::{pin::Pin, sync::Arc};
use tokio::sync::OnceCell;
use tracing::error;

use crate::errors::{ApitapError, Result};

// =========================== Shared SessionContext ========================== //

static SHARED_CTX: OnceCell<Arc<SessionContext>> = OnceCell::const_new();

/// Stream of JSON rows (`Result<Value>`) boxed + pinned for dynamic dispatch.
pub type JsonStreamType = Pin<Box<dyn Stream<Item = Result<serde_json::Value>> + Send + 'static>>;

pub async fn get_shared_context() -> Arc<SessionContext> {
    SHARED_CTX
        .get_or_init(|| async {
            const MEM_LIMIT: usize = 256 * 1024 * 1024;
            let setup_runtime_env = RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::new(GreedyMemoryPool::new(MEM_LIMIT)))
                .build();

            let runtime_env = match setup_runtime_env {
                Ok(rt) => Arc::new(rt),
                Err(e) => {
                    error!(error = %e, "failed to build DataFusion RuntimeEnv; falling back to default");
                    Arc::new(RuntimeEnvBuilder::new().build().unwrap_or_default())
                }
            };

            let session_config = SessionConfig::new()
                .with_target_partitions(1)
                .with_batch_size(2048);

            Arc::new(SessionContext::new_with_config_rt(session_config, runtime_env))
        })
        .await
        .clone()
}
// ========================= RAII for temp table cleanup ====================== //

pub struct SqlDataFrame {
    df: DataFrame,
    ctx: Arc<SessionContext>,
    table_name: String,
}

impl SqlDataFrame {
    #[inline]
    pub fn inner(&self) -> &DataFrame {
        &self.df
    }
}

impl Drop for SqlDataFrame {
    fn drop(&mut self) {
        let _ = self.ctx.deregister_table(&self.table_name);
    }
}

// ============================= JSON → DF / SQL ============================== //

#[async_trait]
pub trait JsonValueExt {
    async fn to_df(&self) -> Result<DataFrame>;
    async fn to_sql(&self, table_name: &str, sql: &str) -> Result<SqlDataFrame>;
}

#[async_trait]
impl JsonValueExt for serde_json::Value {
    async fn to_df(&self) -> Result<DataFrame> {
        let ctx = get_shared_context().await;

        let Self::Array(json_array) = self else {
            return Err(ApitapError::Datafusion(DatafusionArrowError(
                ArrowError::JsonError("Expected JSON array".to_string()),
                None,
            )));
        };
        if json_array.is_empty() {
            return Err(ApitapError::Datafusion(DatafusionArrowError(
                ArrowError::JsonError("Empty JSON array".to_string()),
                None,
            )));
        }

        let fields: Vec<FieldRef> = Vec::<FieldRef>::from_samples(
            json_array,
            TracingOptions::default()
                .allow_null_fields(true)
                .coerce_numbers(true),
        )?;

        let batch: RecordBatch = serde_arrow::to_record_batch(&fields, json_array)?;

        Ok(ctx.read_batch(batch)?)
    }

    async fn to_sql(&self, table_name: &str, sql: &str) -> Result<SqlDataFrame> {
        let ctx = get_shared_context().await;

        let Self::Array(json_array) = self else {
            return Err(ApitapError::Datafusion(DatafusionArrowError(
                ArrowError::JsonError("Expected JSON array".to_string()),
                None,
            )));
        };
        if json_array.is_empty() {
            return Err(ApitapError::Datafusion(DatafusionArrowError(
                ArrowError::JsonError("Empty JSON array".to_string()),
                None,
            )));
        }

        let fields: Vec<FieldRef> = Vec::<FieldRef>::from_samples(
            json_array,
            TracingOptions::default()
                .allow_null_fields(true)
                .coerce_numbers(true),
        )?;

        let batch: RecordBatch = serde_arrow::to_record_batch(&fields, json_array)?;

        // Best-effort cleanup of any existing table with the same name.
        let _ = ctx.deregister_table(table_name);

        ctx.register_batch(table_name, batch)?;

        let df = ctx.sql(sql).await?;

        Ok(SqlDataFrame {
            df,
            ctx,
            table_name: table_name.to_string(),
        })
    }
}

#[async_trait]
pub trait DataFrameExt {
    async fn to_vec<T>(&self) -> Result<Vec<T>>
    where
        T: serde::de::DeserializeOwned + Send;

    async fn to_json(&self) -> Result<serde_json::Value>;

    async fn to_stream(&self) -> Result<JsonStreamType> {
        Ok(Box::pin(stream::empty::<Result<serde_json::Value>>()))
    }
}

#[async_trait]
impl DataFrameExt for DataFrame {
    async fn to_vec<T>(&self) -> Result<Vec<T>>
    where
        T: serde::de::DeserializeOwned + Send,
    {
        use datafusion::physical_plan::SendableRecordBatchStream;

        let mut rb_stream: SendableRecordBatchStream = self.clone().execute_stream().await?;

        let mut out = Vec::<T>::new();
        while let Some(item) = rb_stream.next().await {
            let batch = item.map_err(ApitapError::Datafusion)?;
            let vals: Vec<serde_json::Value> = serde_arrow::from_record_batch(&batch)?;
            let chunk: Vec<T> = serde_json::from_value(serde_json::Value::Array(vals))?;
            out.extend(chunk);
        }
        Ok(out)
    }

    async fn to_stream(&self) -> Result<JsonStreamType> {
        use datafusion::physical_plan::SendableRecordBatchStream;

        let mut rb_stream: SendableRecordBatchStream = self.clone().execute_stream().await?;

        let s = async_stream::try_stream! {
            while let Some(item) = rb_stream.next().await {
                let batch = item?;

                let rows: Vec<serde_json::Value> =
                    serde_arrow::from_record_batch(&batch)?;

                for v in rows {
                    yield v;
                }
            }
        };

        Ok(Box::pin(s))
    }

    async fn to_json(&self) -> Result<serde_json::Value> {
        use datafusion::physical_plan::SendableRecordBatchStream;

        let mut rb_stream: SendableRecordBatchStream = self.clone().execute_stream().await?;

        let mut rows = Vec::<serde_json::Value>::new();
        while let Some(item) = rb_stream.next().await {
            let batch = item?;
            let mut vals: Vec<serde_json::Value> = serde_arrow::from_record_batch(&batch)?;
            rows.append(&mut vals);
        }

        Ok(serde_json::Value::Array(rows))
    }
}

// ============================== Writer Types ================================ //

/// Result of a successful query execution (in-memory)
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub table_name: String,
    pub data: serde_json::Value,
    pub row_count: usize,
}

/// Result of a successful query execution (streaming)
pub struct QueryResultStream {
    pub table_name: String,
    pub data: JsonStreamType,
}

/// Query execution error
#[derive(Debug, Clone)]
pub struct QueryError {
    pub table_name: String,
    pub error: String,
}
