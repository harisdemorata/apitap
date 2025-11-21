// Tests for PostgreSQL Writer
//
// These tests cover:
// - PgType type inference and merging
// - PrimaryKey column extraction
// - Schema analysis from JSON values
// - SQL identifier quoting
// - PostgresWriter configuration

use apitap::writer::postgres::{PgType, PrimaryKey};
use serde_json::json;

// ============================================================================
// PgType Tests
// ============================================================================

#[test]
fn test_pgtype_from_json_null() {
    let value = json!(null);
    assert_eq!(PgType::from_json_value(&value), PgType::Text);
}

#[test]
fn test_pgtype_from_json_boolean() {
    assert_eq!(PgType::from_json_value(&json!(true)), PgType::Boolean);
    assert_eq!(PgType::from_json_value(&json!(false)), PgType::Boolean);
}

#[test]
fn test_pgtype_from_json_integer() {
    assert_eq!(PgType::from_json_value(&json!(42)), PgType::BigInt);
    assert_eq!(PgType::from_json_value(&json!(-100)), PgType::BigInt);
    assert_eq!(PgType::from_json_value(&json!(0)), PgType::BigInt);
}

#[test]
fn test_pgtype_from_json_float() {
    assert_eq!(PgType::from_json_value(&json!(3.5)), PgType::Double);
    assert_eq!(PgType::from_json_value(&json!(-2.5)), PgType::Double);
    assert_eq!(PgType::from_json_value(&json!(0.0)), PgType::Double);
}

#[test]
fn test_pgtype_from_json_string() {
    assert_eq!(PgType::from_json_value(&json!("hello")), PgType::Text);
    assert_eq!(PgType::from_json_value(&json!("")), PgType::Text);
}

#[test]
fn test_pgtype_from_json_array() {
    assert_eq!(PgType::from_json_value(&json!([1, 2, 3])), PgType::Jsonb);
    assert_eq!(PgType::from_json_value(&json!([])), PgType::Jsonb);
}

#[test]
fn test_pgtype_from_json_object() {
    assert_eq!(
        PgType::from_json_value(&json!({"key": "value"})),
        PgType::Jsonb
    );
    assert_eq!(PgType::from_json_value(&json!({})), PgType::Jsonb);
}

#[test]
fn test_pgtype_as_sql() {
    assert_eq!(PgType::Text.as_sql(), "TEXT");
    assert_eq!(PgType::Boolean.as_sql(), "BOOLEAN");
    assert_eq!(PgType::BigInt.as_sql(), "BIGINT");
    assert_eq!(PgType::Double.as_sql(), "DOUBLE PRECISION");
    assert_eq!(PgType::Jsonb.as_sql(), "JSONB");
}

#[test]
fn test_pgtype_merge_same_types() {
    assert_eq!(PgType::Text.merge(&PgType::Text), PgType::Text);
    assert_eq!(PgType::BigInt.merge(&PgType::BigInt), PgType::BigInt);
    assert_eq!(PgType::Double.merge(&PgType::Double), PgType::Double);
    assert_eq!(PgType::Boolean.merge(&PgType::Boolean), PgType::Boolean);
    assert_eq!(PgType::Jsonb.merge(&PgType::Jsonb), PgType::Jsonb);
}

#[test]
fn test_pgtype_merge_with_text() {
    // Text is the most permissive - should always win
    assert_eq!(PgType::Text.merge(&PgType::BigInt), PgType::Text);
    assert_eq!(PgType::BigInt.merge(&PgType::Text), PgType::Text);
    assert_eq!(PgType::Text.merge(&PgType::Boolean), PgType::Text);
    assert_eq!(PgType::Jsonb.merge(&PgType::Text), PgType::Text);
}

#[test]
fn test_pgtype_merge_numeric_types() {
    // BigInt + Double -> Double (to accommodate both)
    assert_eq!(PgType::BigInt.merge(&PgType::Double), PgType::Double);
    assert_eq!(PgType::Double.merge(&PgType::BigInt), PgType::Double);
}

#[test]
fn test_pgtype_merge_incompatible_types() {
    // When types don't match and no clear winner, fallback to Text
    assert_eq!(PgType::Boolean.merge(&PgType::BigInt), PgType::Text);
    assert_eq!(PgType::Jsonb.merge(&PgType::Boolean), PgType::Text);
    assert_eq!(PgType::Double.merge(&PgType::Boolean), PgType::Text);
}

// ============================================================================
// PrimaryKey Tests
// ============================================================================

#[test]
fn test_primary_key_single_columns() {
    let pk = PrimaryKey::Single {
        name: "id".to_string(),
        ty: PgType::BigInt,
    };

    let columns = pk.columns();
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0], "id");
}

#[test]
fn test_primary_key_composite_columns() {
    let pk = PrimaryKey::Composite(vec![
        ("user_id".to_string(), PgType::BigInt),
        ("tenant_id".to_string(), PgType::BigInt),
    ]);

    let columns = pk.columns();
    assert_eq!(columns.len(), 2);
    assert_eq!(columns[0], "user_id");
    assert_eq!(columns[1], "tenant_id");
}

#[test]
fn test_primary_key_composite_empty() {
    let pk = PrimaryKey::Composite(vec![]);
    assert_eq!(pk.columns().len(), 0);
}

// ============================================================================
// Schema Analysis Tests
// ============================================================================

#[test]
fn test_analyze_schema_basic_types() {
    let rows = vec![
        json!({"id": 1, "name": "Alice", "active": true}),
        json!({"id": 2, "name": "Bob", "active": false}),
    ];

    let schema = apitap::writer::postgres::PostgresWriter::analyze_schema(&rows, 10).unwrap();

    assert_eq!(schema.len(), 3);
    assert_eq!(schema.get("id"), Some(&PgType::BigInt));
    assert_eq!(schema.get("name"), Some(&PgType::Text));
    assert_eq!(schema.get("active"), Some(&PgType::Boolean));
}

#[test]
fn test_analyze_schema_type_coercion() {
    // Mix integer and float - should coerce to Double
    let rows = vec![json!({"value": 100}), json!({"value": 45.67})];

    let schema = apitap::writer::postgres::PostgresWriter::analyze_schema(&rows, 10).unwrap();

    assert_eq!(schema.get("value"), Some(&PgType::Double));
}

#[test]
fn test_analyze_schema_complex_types() {
    let rows = vec![json!({"tags": ["rust", "postgres"], "metadata": {"version": 1}})];

    let schema = apitap::writer::postgres::PostgresWriter::analyze_schema(&rows, 10).unwrap();

    assert_eq!(schema.get("tags"), Some(&PgType::Jsonb));
    assert_eq!(schema.get("metadata"), Some(&PgType::Jsonb));
}

#[test]
fn test_analyze_schema_with_nulls() {
    let rows = vec![
        json!({"id": 1, "optional": null}),
        json!({"id": 2, "optional": "value"}),
    ];

    let schema = apitap::writer::postgres::PostgresWriter::analyze_schema(&rows, 10).unwrap();

    // Null should be treated as Text, then merged with actual type
    assert_eq!(schema.get("optional"), Some(&PgType::Text));
}

#[test]
fn test_analyze_schema_empty_rows() {
    let rows: Vec<serde_json::Value> = vec![];

    let result = apitap::writer::postgres::PostgresWriter::analyze_schema(&rows, 10);

    // Empty rows should return empty schema
    assert!(result.is_ok());
    assert_eq!(result.unwrap().len(), 0);
}

#[test]
fn test_analyze_schema_non_object() {
    let rows = vec![json!("not an object")];

    let result = apitap::writer::postgres::PostgresWriter::analyze_schema(&rows, 10);

    // Should return error for non-object JSON
    assert!(result.is_err());
}

#[test]
fn test_analyze_schema_respects_sample_size() {
    let rows = vec![
        json!({"col1": 1}),
        json!({"col1": 2}),
        json!({"col1": 3}),
        json!({"col1": "text"}), // This would coerce to Text if sampled
    ];

    // Sample only first 2 rows
    let schema = apitap::writer::postgres::PostgresWriter::analyze_schema(&rows, 2).unwrap();
    assert_eq!(schema.get("col1"), Some(&PgType::BigInt));

    // Sample all rows
    let schema_all = apitap::writer::postgres::PostgresWriter::analyze_schema(&rows, 10).unwrap();
    assert_eq!(schema_all.get("col1"), Some(&PgType::Text)); // Coerced due to string
}

#[test]
fn test_analyze_schema_column_order_stable() {
    // BTreeMap should maintain stable ordering
    let rows = vec![json!({"z_field": 1, "a_field": "test", "m_field": true})];

    let schema = apitap::writer::postgres::PostgresWriter::analyze_schema(&rows, 10).unwrap();

    let keys: Vec<&String> = schema.keys().collect();
    // BTreeMap sorts keys alphabetically
    assert_eq!(keys, vec!["a_field", "m_field", "z_field"]);
}

// ============================================================================
// SQL Identifier Quoting Tests
// ============================================================================

#[test]
fn test_quote_ident_simple() {
    let quoted = apitap::writer::postgres::PostgresWriter::quote_ident("users");
    assert_eq!(quoted, r#""users""#);
}

#[test]
fn test_quote_ident_with_spaces() {
    let quoted = apitap::writer::postgres::PostgresWriter::quote_ident("user name");
    assert_eq!(quoted, r#""user name""#);
}

#[test]
fn test_quote_ident_with_special_chars() {
    let quoted = apitap::writer::postgres::PostgresWriter::quote_ident("user-id");
    assert_eq!(quoted, r#""user-id""#);
}

#[test]
fn test_quote_ident_with_embedded_quotes() {
    // Double quotes should be escaped by doubling them
    let quoted = apitap::writer::postgres::PostgresWriter::quote_ident(r#"user"name"#);
    // Input: user"name
    // Each " should become ""
    // Output should be: "user""name"
    assert_eq!(quoted, r#""user""name""#);
}

#[test]
fn test_quote_ident_empty() {
    let quoted = apitap::writer::postgres::PostgresWriter::quote_ident("");
    assert_eq!(quoted, r#""""#);
}

#[test]
fn test_quote_ident_path_simple() {
    let quoted = apitap::writer::postgres::PostgresWriter::quote_ident_path("public.users");
    assert_eq!(quoted, r#""public"."users""#);
}

#[test]
fn test_quote_ident_path_three_parts() {
    let quoted = apitap::writer::postgres::PostgresWriter::quote_ident_path("mydb.public.users");
    assert_eq!(quoted, r#""mydb"."public"."users""#);
}

#[test]
fn test_quote_ident_path_single_name() {
    let quoted = apitap::writer::postgres::PostgresWriter::quote_ident_path("users");
    assert_eq!(quoted, r#""users""#);
}

#[test]
fn test_quote_ident_path_with_special_chars() {
    let quoted = apitap::writer::postgres::PostgresWriter::quote_ident_path("my-schema.user_table");
    assert_eq!(quoted, r#""my-schema"."user_table""#);
}

// ============================================================================
// PostgresWriter Configuration Tests
// ============================================================================

// NOTE: PostgresWriter tests requiring PgPool connections have been removed.
// These tests require:
// - A running PostgreSQL database
// - Tokio runtime context
// - Proper async test setup
//
// Integration tests for PostgresWriter should cover:
// Those would require:
// - Docker container with PostgreSQL
// - Test database setup/teardown
// - Mock database connections
//
// Future integration tests should cover:
// - table_exists()
// - create_table_from_schema()
// - truncate()
// - insert_batch()
// - merge_batch()
// - write() / write_stream()
// - Transaction methods (begin/commit/rollback)
