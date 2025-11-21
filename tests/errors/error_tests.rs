use apitap::errors::{ApitapError, Result};
use std::io;

#[test]
fn test_config_error_display() {
    let err = ApitapError::ConfigError("missing configuration".to_string());
    assert_eq!(
        err.to_string(),
        "Configuration error: missing configuration"
    );
}

#[test]
fn test_writer_error_display() {
    let err = ApitapError::WriterError("connection failed".to_string());
    assert_eq!(err.to_string(), "Writer error: connection failed");
}

#[test]
fn test_pipeline_error_display() {
    let err = ApitapError::PipelineError("failed to process data".to_string());
    assert_eq!(err.to_string(), "Pipeline error: failed to process data");
}

#[test]
fn test_pagination_error_display() {
    let err = ApitapError::PaginationError("invalid cursor".to_string());
    assert_eq!(err.to_string(), "Pagination error: invalid cursor");
}

#[test]
fn test_unsupported_sink_display() {
    let err = ApitapError::UnsupportedSink("mysql".to_string());
    assert_eq!(err.to_string(), "Unsupported sink: mysql");
}

#[test]
fn test_merge_error_display() {
    let err = ApitapError::MergeError("duplicate keys detected".to_string());
    assert_eq!(err.to_string(), "Merge Error: duplicate keys detected");
}

#[test]
fn test_poison_error_display() {
    let err = ApitapError::PoisonError("lock poisoned".to_string());
    assert_eq!(err.to_string(), "Poison Error: lock poisoned");
}

#[test]
fn test_io_error_from_conversion() {
    let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
    let apitap_err: ApitapError = io_err.into();

    assert!(apitap_err.to_string().contains("I/O error"));
}

#[test]
fn test_serde_json_error_from_conversion() {
    let json_str = "{invalid json}";
    let json_err = serde_json::from_str::<serde_json::Value>(json_str).unwrap_err();
    let apitap_err: ApitapError = json_err.into();

    assert!(apitap_err.to_string().contains("JSON serialization error"));
}

#[test]
fn test_result_type_ok() {
    let result: Result<i32> = Ok(42);
    assert!(result.is_ok());
    if let Ok(val) = result {
        assert_eq!(val, 42);
    } else {
        panic!("Expected Ok value");
    }
}

#[test]
fn test_result_type_err() {
    let result: Result<i32> = Err(ApitapError::ConfigError("test error".to_string()));
    assert!(result.is_err());
}

#[test]
fn test_error_debug_format() {
    let err = ApitapError::ConfigError("debug test".to_string());
    let debug_str = format!("{:?}", err);
    assert!(debug_str.contains("ConfigError"));
}

#[test]
fn test_url_parse_error_from_conversion() {
    let url_err = url::Url::parse("not a valid url").unwrap_err();
    let apitap_err: ApitapError = url_err.into();

    assert!(apitap_err.to_string().contains("URL parse error"));
}

#[test]
fn test_multiple_error_types_in_chain() {
    fn do_something() -> Result<()> {
        Err(ApitapError::ConfigError("first error".to_string()))
    }

    fn handle_error() -> Result<()> {
        do_something()?;
        Ok(())
    }

    let result = handle_error();
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Configuration error"));
}

#[test]
fn test_error_contains_provides_context() {
    let err = ApitapError::WriterError("Database connection timeout after 30 seconds".to_string());
    let err_str = err.to_string();

    assert!(err_str.contains("Writer error"));
    assert!(err_str.contains("connection timeout"));
}
