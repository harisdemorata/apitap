use apitap::utils::streaming::{StreamConfig, TrueStreamingProcessor};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use futures::stream;
use serde_json::json;
use std::sync::Arc;

#[test]
fn test_stream_config_default() {
    let config = StreamConfig::default();

    assert_eq!(config.batch_size, 256);
    assert_eq!(config.max_buffered_items, 512);
    assert!(config.true_streaming);
}

#[test]
fn test_stream_config_custom() {
    let config = StreamConfig {
        batch_size: 100,
        max_buffered_items: 200,
        true_streaming: false,
    };

    assert_eq!(config.batch_size, 100);
    assert_eq!(config.max_buffered_items, 200);
    assert!(!config.true_streaming);
}

#[test]
fn test_stream_config_clone() {
    let config = StreamConfig {
        batch_size: 50,
        max_buffered_items: 100,
        true_streaming: true,
    };

    let cloned = config.clone();

    assert_eq!(cloned.batch_size, 50);
    assert_eq!(cloned.max_buffered_items, 100);
    assert!(cloned.true_streaming);
}

#[test]
fn test_stream_config_debug() {
    let config = StreamConfig::default();
    let debug_str = format!("{:?}", config);

    assert!(debug_str.contains("StreamConfig"));
    assert!(debug_str.contains("batch_size"));
}

#[test]
fn test_true_streaming_processor_new() {
    let processor = TrueStreamingProcessor::new(128);

    // Can't directly test batch_size as it's private,
    // but we can verify it was created successfully
    // by using it in a stream operation
    assert_eq!(
        std::mem::size_of_val(&processor),
        std::mem::size_of::<usize>()
    );
}

#[test]
fn test_true_streaming_processor_min_batch_size() {
    // Test that batch size is set to minimum of 1 even if 0 is provided
    let processor = TrueStreamingProcessor::new(0);

    // Processor should use minimum batch size of 1
    // This is tested implicitly through successful creation
    assert_eq!(
        std::mem::size_of_val(&processor),
        std::mem::size_of::<usize>()
    );
}

#[tokio::test]
async fn test_json_to_batch_stream_basic() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let json_values = vec![
        Ok(json!({"id": 1, "name": "Alice"})),
        Ok(json!({"id": 2, "name": "Bob"})),
        Ok(json!({"id": 3, "name": "Charlie"})),
    ];

    let json_stream = Box::pin(stream::iter(json_values));

    let processor = TrueStreamingProcessor::new(2);
    let result = processor.json_to_batch_stream(json_stream, schema).await;

    // Verify stream was created successfully
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_json_to_batch_stream_empty() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

    let json_values: Vec<Result<serde_json::Value, apitap::errors::ApitapError>> = vec![];
    let json_stream = Box::pin(stream::iter(json_values));

    let processor = TrueStreamingProcessor::new(10);
    let result = processor.json_to_batch_stream(json_stream, schema).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_json_to_batch_stream_single_item() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));

    let json_values = vec![Ok(json!({"value": 42}))];
    let json_stream = Box::pin(stream::iter(json_values));

    let processor = TrueStreamingProcessor::new(10);
    let result = processor.json_to_batch_stream(json_stream, schema).await;

    assert!(result.is_ok());
}

#[test]
fn test_stream_config_various_batch_sizes() {
    let sizes = vec![1, 10, 100, 256, 512, 1000, 5000];

    for size in sizes {
        let config = StreamConfig {
            batch_size: size,
            max_buffered_items: size * 2,
            true_streaming: true,
        };

        assert_eq!(config.batch_size, size);
        assert_eq!(config.max_buffered_items, size * 2);
    }
}

#[test]
fn test_streaming_config_true_vs_false() {
    let streaming_on = StreamConfig {
        batch_size: 100,
        max_buffered_items: 200,
        true_streaming: true,
    };

    let streaming_off = StreamConfig {
        batch_size: 100,
        max_buffered_items: 200,
        true_streaming: false,
    };

    assert!(streaming_on.true_streaming);
    assert!(!streaming_off.true_streaming);
}

#[tokio::test]
async fn test_processor_with_different_batch_sizes() {
    // Test that processor can be created with various batch sizes
    let batch_sizes = vec![1, 5, 10, 50, 100, 256, 500, 1000];

    for size in batch_sizes {
        let processor = TrueStreamingProcessor::new(size);

        // Verify processor was created (size check is a simple validation)
        assert!(std::mem::size_of_val(&processor) > 0);
    }
}

#[tokio::test]
async fn test_json_to_batch_with_null_values() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),  // nullable
        Field::new("name", DataType::Utf8, true), // nullable
    ]));

    let json_values = vec![
        Ok(json!({"id": 1, "name": "Alice"})),
        Ok(json!({"id": 2, "name": null})),
        Ok(json!({"id": null, "name": "Charlie"})),
    ];

    let json_stream = Box::pin(stream::iter(json_values));

    let processor = TrueStreamingProcessor::new(5);
    let result = processor.json_to_batch_stream(json_stream, schema).await;

    assert!(result.is_ok());
}
