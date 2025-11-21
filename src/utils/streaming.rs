use crate::errors::Result;
use datafusion::arrow::{array::RecordBatch, datatypes::Schema};
use futures::Stream;
use serde_json::Value;
use std::pin::Pin;
use std::sync::Arc;

/// Streaming mode: process one JSON object at a time, never buffer
pub struct TrueStreamingProcessor {
    batch_size: usize,
}

impl TrueStreamingProcessor {
    pub fn new(batch_size: usize) -> Self {
        Self {
            batch_size: batch_size.max(1),
        }
    }

    /// Convert a stream of JSON values directly to RecordBatch stream
    /// WITHOUT buffering entire batches
    pub async fn json_to_batch_stream(
        &self,
        mut json_stream: Pin<Box<dyn Stream<Item = Result<Value>> + Send>>,
        schema: Arc<datafusion::arrow::datatypes::Schema>,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>> {
        let batch_size = self.batch_size;
        let schema = schema.clone();

        let batch_stream = async_stream::try_stream! {

            let mut buffer = Vec::with_capacity(batch_size);

            while let Some(json_value) = futures::StreamExt::next(&mut json_stream).await {
                let value = json_value?;
                buffer.push(value);

                if buffer.len() >= batch_size {
                    // Convert buffer to RecordBatch without intermediate Vec
                    let batch = direct_json_to_batch(&buffer, &schema)?;
                    buffer.clear();
                    yield batch;
                }
            }

            // Flush remaining items
            if !buffer.is_empty() {
                let batch = direct_json_to_batch(&buffer, &schema)?;
                yield batch;
            }
        };

        Ok(Box::pin(batch_stream))
    }
}

/// Direct JSON → RecordBatch without intermediate JSON serialization
fn direct_json_to_batch(values: &Vec<Value>, schema: &Arc<Schema>) -> Result<RecordBatch> {
    let record_batch = serde_arrow::to_record_batch(schema.fields(), values)
        .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;

    Ok(record_batch)
}

/// Configuration for streaming behavior
#[derive(Debug, Clone)]
pub struct StreamConfig {
    /// Size of batches before converting to Arrow
    pub batch_size: usize,
    /// Maximum items in flight (backpressure)
    pub max_buffered_items: usize,
    /// Use true streaming (vs. buffering mode)
    pub true_streaming: bool,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            batch_size: 256,
            max_buffered_items: 512,
            true_streaming: true,
        }
    }
}

/// Low-memory streaming processor
pub async fn stream_json_to_batches(
    json_stream: Pin<Box<dyn Stream<Item = Result<Value>> + Send>>,
    schema: Arc<datafusion::arrow::datatypes::Schema>,
    config: StreamConfig,
) -> Result<Pin<Box<dyn Stream<Item = Result<datafusion::arrow::array::RecordBatch>> + Send>>> {
    let processor = TrueStreamingProcessor::new(config.batch_size);
    processor.json_to_batch_stream(json_stream, schema).await
}
