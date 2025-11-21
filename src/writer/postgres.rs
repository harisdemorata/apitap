// src/utils/postgres_writer.rs

use crate::errors::{ApitapError, Result};
use crate::utils::datafusion_ext::{QueryResult, QueryResultStream};
use crate::writer::{DataWriter, WriteMode};
use async_trait::async_trait;
use serde_json::Value;
use sqlx::{types::Json, PgPool};
use std::borrow::Cow;
use std::collections::BTreeMap;
use tokio_stream::StreamExt;
use tracing::{debug, debug_span, info};

//=============== Type Definitions ============================================//

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgType {
    Text,
    Boolean,
    BigInt,
    Double,
    Jsonb,
}

impl PgType {
    pub fn as_sql(&self) -> &'static str {
        match self {
            PgType::Text => "TEXT",
            PgType::Boolean => "BOOLEAN",
            PgType::BigInt => "BIGINT",
            PgType::Double => "DOUBLE PRECISION",
            PgType::Jsonb => "JSONB",
        }
    }

    pub fn from_json_value(value: &Value) -> Self {
        match value {
            Value::Null => PgType::Text,
            Value::Bool(_) => PgType::Boolean,
            Value::Number(n) => {
                if n.is_i64() {
                    PgType::BigInt
                } else {
                    PgType::Double
                }
            }
            Value::String(_) => PgType::Text,
            Value::Array(_) => PgType::Jsonb,
            Value::Object(_) => PgType::Jsonb,
        }
    }

    pub fn merge(&self, other: &Self) -> Self {
        match (self, other) {
            (PgType::Text, _) | (_, PgType::Text) => PgType::Text,
            (PgType::BigInt, PgType::Double) | (PgType::Double, PgType::BigInt) => PgType::Double,
            (PgType::BigInt, PgType::BigInt) => PgType::BigInt,
            (PgType::Double, PgType::Double) => PgType::Double,
            (a, b) if a == b => *a,
            _ => PgType::Text,
        }
    }
}

//=============== PostgreSQL Auto-Columns Writer ==============================//

#[derive(Debug, Clone)]
pub enum PrimaryKey {
    /// Single-column PK with an explicit postgres type
    Single { name: String, ty: PgType },
    /// Multi-column PK (composite)
    Composite(Vec<(String, PgType)>),
}

impl PrimaryKey {
    pub fn columns(&self) -> Vec<&str> {
        match self {
            PrimaryKey::Single { name, .. } => vec![name.as_str()],
            PrimaryKey::Composite(cols) => cols.iter().map(|(n, _)| n.as_str()).collect(),
        }
    }
}

pub struct PostgresWriter {
    pool: PgPool,
    pub table_name: String,
    pub batch_size: usize,
    pub sample_size: usize,
    pub auto_create: bool,
    pub auto_truncate: bool,
    columns_cache: tokio::sync::RwLock<Option<BTreeMap<String, PgType>>>,
    pub primary_key: Option<String>,
    version_cache: tokio::sync::RwLock<Option<PostgresVersion>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct PostgresVersion {
    pub major: u32,
    pub minor: u32,
}

impl PostgresVersion {
    pub fn new(major: u32, minor: u32) -> Self {
        Self { major, minor }
    }

    pub fn supports_merge(&self) -> bool {
        self.major >= 15
    }

    pub fn supports_upsert(&self) -> bool {
        // INSERT ... ON CONFLICT is available since PostgreSQL 9.5
        self.major >= 9 && (self.major > 9 || self.minor >= 5)
    }

    pub fn parse(version_str: &str) -> Result<Self> {
        // PostgreSQL version string format: "PostgreSQL X.Y.Z on ..."
        // or "X.Y.Z" or just "X.Y"
        let version_part = version_str
            .split_whitespace()
            .find(|s| {
                s.chars()
                    .next()
                    .map(|c| c.is_ascii_digit())
                    .unwrap_or(false)
            })
            .or_else(|| version_str.split_whitespace().nth(1))
            .unwrap_or(version_str);

        let parts: Vec<&str> = version_part.split('.').collect();
        if parts.is_empty() {
            return Err(ApitapError::PipelineError(format!(
                "Invalid PostgreSQL version format: {}",
                version_str
            )));
        }

        let major = parts[0].parse::<u32>().map_err(|_| {
            ApitapError::PipelineError(format!("Invalid major version: {}", parts[0]))
        })?;

        let minor = if parts.len() > 1 {
            parts[1].parse::<u32>().unwrap_or(0)
        } else {
            0
        };

        Ok(Self { major, minor })
    }
}

impl std::fmt::Display for PostgresVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

impl PostgresWriter {
    pub fn new(pool: PgPool, table_name: impl Into<String>) -> Self {
        Self {
            pool,
            table_name: table_name.into(),
            batch_size: 5000, // Increased from 100 to 5000 for better performance
            sample_size: 10,
            auto_create: true,
            auto_truncate: false,
            columns_cache: tokio::sync::RwLock::new(None),
            primary_key: None,
            version_cache: tokio::sync::RwLock::new(None),
        }
    }

    pub fn with_primary_key_single(mut self, name: impl Into<Option<String>>) -> Self {
        self.primary_key = name.into();
        self
    }

    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    pub fn with_sample_size(mut self, size: usize) -> Self {
        self.sample_size = size;
        self
    }

    pub fn auto_create(mut self, enabled: bool) -> Self {
        self.auto_create = enabled;
        self
    }

    pub fn auto_truncate(mut self, enabled: bool) -> Self {
        self.auto_truncate = enabled;
        self
    }

    async fn table_exists(&self) -> Result<bool> {
        let result: (bool,) = sqlx::query_as(
            "SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = $1
            )",
        )
        .bind(&self.table_name)
        .fetch_one(&self.pool)
        .await?;

        Ok(result.0)
    }

    pub fn analyze_schema(rows: &[Value], sample_size: usize) -> Result<BTreeMap<String, PgType>> {
        let mut column_types: BTreeMap<String, Vec<PgType>> = BTreeMap::new();

        let sample = &rows[..rows.len().min(sample_size)];

        for row in sample {
            let obj = row
                .as_object()
                .ok_or_else(|| ApitapError::PipelineError("Expected JSON object".to_string()))?;

            for (key, value) in obj {
                let pg_type = PgType::from_json_value(value);
                column_types.entry(key.clone()).or_default().push(pg_type);
            }
        }

        let mut final_types = BTreeMap::new();
        for (col_name, types) in column_types {
            let final_type = types.iter().fold(types[0], |acc, t| acc.merge(t));
            final_types.insert(col_name, final_type);
        }

        Ok(final_types)
    }

    pub fn quote_ident(ident: &str) -> String {
        // "foo"  -> "\"foo\""
        // handle embedded quotes: foo"bar -> "foo""bar"
        format!(r#""{}""#, ident.replace('"', r#""""#))
    }

    pub fn quote_ident_path(path: &str) -> String {
        // public.unplash -> "public"."unplash"
        path.split('.')
            .map(Self::quote_ident)
            .collect::<Vec<_>>()
            .join(".")
    }

    pub async fn create_table_from_schema(&self, schema: &BTreeMap<String, PgType>) -> Result<()> {
        if schema.is_empty() {
            return Err(ApitapError::PipelineError(
                "No columns detected".to_string(),
            ));
        }

        let column_defs: Vec<String> = schema
            .iter()
            .map(|(name, pg_type)| format!(r#"{} {}"#, Self::quote_ident(name), pg_type.as_sql()))
            .collect();

        let pk_clause: Option<String> = match &self.primary_key {
            Some(pk_name) => {
                if schema.contains_key(pk_name) {
                    Some(format!(r#"PRIMARY KEY ({})"#, Self::quote_ident(pk_name)))
                } else {
                    tracing::warn!(
                        "Primary key '{}' not found in schema for table '{}'; creating without PK",
                        pk_name,
                        self.table_name
                    );
                    None
                }
            }
            None => None,
        };

        let mut all_parts = column_defs;
        if let Some(pk) = pk_clause {
            all_parts.push(pk);
        }

        let table_sql = Self::quote_ident_path(&self.table_name);
        let query = format!(
            "CREATE TABLE IF NOT EXISTS {} (\n    {}\n)",
            table_sql,
            all_parts.join(",\n    ")
        );
        // Execute CREATE TABLE and instrument with a debug span
        let span = debug_span!("sql.execute", statement = "create_table", table = %self.table_name);
        let _g = span.enter();
        let res = sqlx::query(&query).execute(&self.pool).await?;
        debug!(rows_affected = res.rows_affected(), "create_table executed");

        let column_names: Vec<String> = schema.keys().cloned().collect();
        tracing::info!(table = %self.table_name, columns = column_names.len(), cols = %column_names.join(", "), "created table");
        tracing::info!("column types:");
        for (name, pg_type) in schema {
            tracing::info!(column = %name, typ = %pg_type.as_sql(), "column type");
        }

        Ok(())
    }

    async fn ensure_table(&self, sample_rows: &[Value]) -> Result<BTreeMap<String, PgType>> {
        if let Some(schema) = self.columns_cache.read().await.as_ref() {
            return Ok(schema.clone());
        }

        let schema = if !self.table_exists().await? {
            if self.auto_create {
                if sample_rows.is_empty() {
                    return Err(ApitapError::PipelineError(
                        "Need sample data to create table".to_string(),
                    ));
                }
                let detected_schema = Self::analyze_schema(sample_rows, self.sample_size)?;
                self.create_table_from_schema(&detected_schema).await?;
                detected_schema
            } else {
                return Err(ApitapError::PipelineError(format!(
                    "Table '{}' does not exist",
                    self.table_name
                )));
            }
        } else {
            if sample_rows.is_empty() {
                return Err(ApitapError::PipelineError("Need sample data".to_string()));
            }
            Self::analyze_schema(sample_rows, self.sample_size)?
        };

        *self.columns_cache.write().await = Some(schema.clone());

        Ok(schema)
    }

    /// Fetch and cache the PostgreSQL server version
    pub async fn get_postgres_version(&self) -> Result<PostgresVersion> {
        // Check cache first
        if let Some(version) = *self.version_cache.read().await {
            return Ok(version);
        }

        // Fetch version from database
        let version_row: (String,) = sqlx::query_as("SELECT version()")
            .fetch_one(&self.pool)
            .await?;

        let version = PostgresVersion::parse(&version_row.0)?;

        // Cache the version
        *self.version_cache.write().await = Some(version);

        tracing::info!(
            version = %version,
            supports_merge = version.supports_merge(),
            "detected PostgreSQL version"
        );

        Ok(version)
    }

    pub async fn truncate(&self) -> Result<()> {
        let table_sql = Self::quote_ident(&self.table_name);
        let sql = format!("TRUNCATE TABLE {}", table_sql);

        tracing::info!(table = %self.table_name, "truncating table");
        tracing::debug!(sql = %sql, "truncate sql");

        let res = {
            let span = debug_span!("sql.execute", statement = "truncate", table = %self.table_name);
            let _g = span.enter();
            sqlx::query(&sql).execute(&self.pool).await
        };
        match res {
            Ok(res) => {
                debug!(rows_affected = res.rows_affected(), "truncate executed");
                Ok(())
            }
            Err(e) => {
                // emulate IF EXISTS: swallow "undefined_table" (42P01)
                if let Some(db_err) = e.as_database_error() {
                    if db_err.code() == Some(Cow::Borrowed("42P01")) {
                        tracing::error!(table = %self.table_name, "table does not exist, skipping TRUNCATE");
                        return Ok(());
                    }
                }
                Err(ApitapError::PipelineError(format!("TRUNCATE: {}", e)))
            }
        }
    }

    /// Upsert batch using INSERT ... ON CONFLICT DO UPDATE (PostgreSQL 9.5+)
    /// This is used for PostgreSQL versions < 15 that don't support MERGE
    pub async fn upsert_batch(
        &self,
        rows: &[Value],
        schema: &BTreeMap<String, PgType>,
    ) -> Result<()> {
        // ---- Guards ------------------------------------------------------------
        if rows.is_empty() {
            info!(table = %self.table_name, "upsert_batch: no rows to upsert; skipping");
            return Ok(());
        }
        if schema.is_empty() {
            return Err(ApitapError::MergeError("No columns detected".to_string()));
        }

        let pk_name = self.primary_key.clone().ok_or_else(|| {
            ApitapError::MergeError("Postgres: primary key not configured".to_string())
        })?;

        // Column lists (BTreeMap keeps stable order)
        let col_names_raw: Vec<&str> = schema.keys().map(|s| s.as_str()).collect();
        let values_per_row = col_names_raw.len();
        let col_names_sql: Vec<String> =
            col_names_raw.iter().map(|n| Self::quote_ident(n)).collect();

        let columns_str = col_names_sql.join(", ");

        // Build placeholders for VALUES
        let mut placeholders = Vec::with_capacity(rows.len());
        for row_idx in 0..rows.len() {
            let row_ph: Vec<String> = (1..=values_per_row)
                .map(|col_idx| format!("${}", row_idx * values_per_row + col_idx))
                .collect();
            placeholders.push(format!("({})", row_ph.join(", ")));
        }

        let table_sql = Self::quote_ident_path(&self.table_name);
        let pk_quoted = Self::quote_ident(&pk_name);

        // Determine non-PK columns for UPDATE clause
        let non_pk_cols: Vec<&str> = col_names_raw
            .iter()
            .filter(|c| **c != pk_name.as_str())
            .copied()
            .collect();

        // Build UPDATE SET clause: "col" = EXCLUDED."col"
        let update_set = if non_pk_cols.is_empty() {
            // If only PK, do nothing on conflict (just ensures uniqueness)
            "".to_string()
        } else {
            let assignments: Vec<String> = non_pk_cols
                .iter()
                .map(|col| {
                    format!(
                        "{} = EXCLUDED.{}",
                        Self::quote_ident(col),
                        Self::quote_ident(col)
                    )
                })
                .collect();
            format!("DO UPDATE SET {}", assignments.join(", "))
        };

        let conflict_clause = if non_pk_cols.is_empty() {
            format!("ON CONFLICT ({}) DO NOTHING", pk_quoted)
        } else {
            format!("ON CONFLICT ({}) {}", pk_quoted, update_set)
        };

        let query = format!(
            "INSERT INTO {} ({}) VALUES {} {}",
            table_sql,
            columns_str,
            placeholders.join(", "),
            conflict_clause
        );

        debug!(
            table = %table_sql,
            pk = %pk_name,
            rows = rows.len(),
            cols = values_per_row,
            will_update_cols = non_pk_cols.len(),
            "UPSERT batch details"
        );
        debug!(%query, "UPSERT SQL");

        // Bind values
        let mut all_values = Vec::with_capacity(rows.len() * values_per_row);
        for row in rows {
            for col in &col_names_raw {
                all_values.push(row.get(*col).cloned().unwrap_or(Value::Null));
            }
        }

        let mut q = sqlx::query(&query);
        for (idx, value) in all_values.iter().enumerate() {
            let col_idx = idx % values_per_row;
            let col_name = col_names_raw[col_idx];
            let expected = schema
                .get(col_name)
                .expect("schema must contain column present in rows");
            q = self.bind_value(q, value, expected)?;
        }

        // Execute
        let span = debug_span!("sql.execute", statement = "upsert", table = %self.table_name, batch_rows = rows.len());
        let _g = span.enter();
        let res = q.execute(&self.pool).await?;
        debug!(rows_affected = res.rows_affected(), "upsert executed");

        Ok(())
    }

    pub async fn merge_batch(
        &self,
        rows: &[Value],
        schema: &BTreeMap<String, PgType>,
    ) -> Result<()> {
        // ---- Version Detection -------------------------------------------------
        // Choose implementation based on PostgreSQL version
        let version = self.get_postgres_version().await?;

        if version.supports_merge() {
            // Use MERGE for PostgreSQL 15+
            return self.merge_batch_pg15(rows, schema).await;
        } else if version.supports_upsert() {
            // Use INSERT ... ON CONFLICT for PostgreSQL 9.5-14
            return self.upsert_batch(rows, schema).await;
        } else {
            Err(ApitapError::MergeError(format!(
                "Merge operation requires PostgreSQL 9.5 or higher (detected version: {})",
                version
            )))
        }
    }

    /// MERGE implementation for PostgreSQL 15+
    async fn merge_batch_pg15(
        &self,
        rows: &[Value],
        schema: &BTreeMap<String, PgType>,
    ) -> Result<()> {
        // ---- Guards ------------------------------------------------------------
        // Process rows without logging each individual row for performance
        if rows.is_empty() {
            info!(table = %self.table_name, "merge_batch: no rows to merge; skipping");
            return Ok(());
        }
        if schema.is_empty() {
            return Err(ApitapError::MergeError("No columns detected".to_string()));
        }

        let pk_name = self.primary_key.clone().ok_or_else(|| {
            ApitapError::MergeError("Postgres: primary key not configured".to_string())
        })?;

        // Column lists (BTreeMap keeps stable order)
        let col_names_raw: Vec<&str> = schema.keys().map(|s| s.as_str()).collect();
        let values_per_row = col_names_raw.len();

        // Quoted names for target table (t."col") and source alias (s."col")
        let cols_t_quoted: Vec<String> =
            col_names_raw.iter().map(|c| Self::quote_ident(c)).collect();
        let cols_s_quoted: Vec<String> = col_names_raw
            .iter()
            .map(|c| format!("s.{}", Self::quote_ident(c)))
            .collect();

        // Helper strings
        let columns_t_str = cols_t_quoted.join(", ");
        let columns_s_str = cols_s_quoted.join(", ");
        let using_cols_str = cols_t_quoted.join(", "); // names for s(...)

        // VALUES placeholders
        let mut placeholders = Vec::with_capacity(rows.len());
        for row_idx in 0..rows.len() {
            let row_ph: Vec<String> = (1..=values_per_row)
                .map(|col_idx| format!("${}", row_idx * values_per_row + col_idx))
                .collect();
            placeholders.push(format!("({})", row_ph.join(", ")));
        }
        let values_block = placeholders.join(",\n        ");

        // Target table + PK refs
        let table_sql = Self::quote_ident_path(&self.table_name);
        let pk_t = format!(r#"t.{}"#, Self::quote_ident(&pk_name));
        let pk_s = format!(r#"s.{}"#, Self::quote_ident(&pk_name));

        // Determine non-PK columns
        let non_pk_idx: Vec<usize> = col_names_raw
            .iter()
            .enumerate()
            .filter(|(_, c)| **c != pk_name.as_str())
            .map(|(i, _)| i)
            .collect();

        // Build the UPDATE clause with correct Postgres forms:
        //  - 0 cols: no UPDATE
        //  - 1 col:  UPDATE SET t."c" = s."c"
        //  - >1:     UPDATE SET (t."c1", t."c2") = ROW(s."c1", s."c2")
        let set_clause = if non_pk_idx.is_empty() {
            None
        } else if non_pk_idx.len() == 1 {
            let i = non_pk_idx[0];
            let tcol = &cols_t_quoted[i];
            let scol = &cols_s_quoted[i];
            Some(format!(r#"UPDATE SET {tcol} = {scol}"#))
        } else {
            let left = non_pk_idx
                .iter()
                .map(|&i| &cols_t_quoted[i])
                .cloned()
                .collect::<Vec<_>>()
                .join(", ");
            let right = non_pk_idx
                .iter()
                .map(|&i| &cols_s_quoted[i])
                .cloned()
                .collect::<Vec<_>>()
                .join(", ");
            Some(format!(r#"UPDATE SET ({left}) = ROW({right})"#))
        };

        // Final SQL
        let query = match set_clause {
            Some(set) => format!(
                r#"
MERGE INTO {table} AS t
USING (VALUES
        {values}
) AS s({using_cols})
ON {pk_t} = {pk_s}
WHEN MATCHED THEN
  {set}
WHEN NOT MATCHED THEN
  INSERT ({cols})
  VALUES ({cols_s});
"#,
                table = table_sql,
                values = values_block,
                using_cols = using_cols_str,
                pk_t = pk_t,
                pk_s = pk_s,
                set = set,
                cols = columns_t_str,
                cols_s = columns_s_str,
            ),
            None => format!(
                r#"
MERGE INTO {table} AS t
USING (VALUES
        {values}
) AS s({using_cols})
ON {pk_t} = {pk_s}
WHEN NOT MATCHED THEN
  INSERT ({cols})
  VALUES ({cols_s});
"#,
                table = table_sql,
                values = values_block,
                using_cols = using_cols_str,
                pk_t = pk_t,
                pk_s = pk_s,
                cols = columns_t_str,
                cols_s = columns_s_str,
            ),
        };

        // Log concise info at INFO, details at DEBUG
        debug!(
            table = %table_sql,
            pk = %pk_name,
            rows = rows.len(),
            cols = values_per_row,
            placeholders = rows.len() * values_per_row,
            will_update_cols = non_pk_idx.len(),
            "MERGE batch details"
        );
        debug!(%query, "MERGE SQL");

        // Bind values row-wise, column order same as schema
        let mut all_values = Vec::with_capacity(rows.len() * values_per_row);
        for row in rows {
            for col in &col_names_raw {
                all_values.push(row.get(*col).cloned().unwrap_or(Value::Null));
            }
        }

        let mut q = sqlx::query(&query);
        for (idx, value) in all_values.iter().enumerate() {
            let col_idx = idx % values_per_row;
            let col_name = col_names_raw[col_idx];
            let expected = schema
                .get(col_name)
                .expect("schema must contain column present in rows");
            q = self.bind_value(q, value, expected)?;
        }

        // Execute
        // Instrument the MERGE execution and log rows_affected
        let span = debug_span!("sql.execute", statement = "merge", table = %self.table_name, batch_rows = rows.len());
        let _g = span.enter();
        let res = q.execute(&self.pool).await?;
        debug!(rows_affected = res.rows_affected(), "merge executed");

        Ok(())
    }

    pub async fn insert_batch(
        &self,
        rows: &[Value],
        schema: &BTreeMap<String, PgType>,
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        // Raw column names (as present in JSON & schema)
        let col_names_raw: Vec<&str> = schema.keys().map(|s| s.as_str()).collect();
        // SQL-safe (quoted) column names for the statement
        let col_names_sql: Vec<String> =
            col_names_raw.iter().map(|n| Self::quote_ident(n)).collect();

        let columns_str = col_names_sql.join(", ");
        let values_per_row = col_names_raw.len();

        // Build placeholders: ($1, $2, ...), ($n+1, ...)
        let mut placeholders = Vec::with_capacity(rows.len());
        for row_idx in 0..rows.len() {
            let row_placeholders: Vec<String> = (1..=values_per_row)
                .map(|col_idx| format!("${}", row_idx * values_per_row + col_idx))
                .collect();
            placeholders.push(format!("({})", row_placeholders.join(", ")));
        }

        // Quote table name too
        let table_sql = Self::quote_ident_path(&self.table_name);

        let query = format!(
            "INSERT INTO {} ({}) VALUES {}",
            table_sql,
            columns_str,
            placeholders.join(", ")
        );

        // Collect values in column order for each row
        let mut all_values = Vec::with_capacity(rows.len() * values_per_row);
        for row in rows {
            for col_name in &col_names_raw {
                let value = row.get(*col_name).cloned().unwrap_or(Value::Null);
                all_values.push(value);
            }
        }

        // Bind in the same order as placeholders
        let mut q = sqlx::query(&query);
        for (idx, value) in all_values.iter().enumerate() {
            let col_idx = idx % values_per_row;
            let col_name = col_names_raw[col_idx];
            let expected_type = schema.get(col_name).expect("schema must contain column");

            q = self.bind_value(q, value, expected_type)?;
        }

        // Instrument the insert execution and log rows_affected
        let span = debug_span!("sql.execute", statement = "insert", table = %self.table_name, batch_rows = rows.len());
        let _g = span.enter();
        let res = q.execute(&self.pool).await?;
        debug!(rows_affected = res.rows_affected(), "insert executed");

        Ok(())
    }

    /// Bind value with proper type conversion
    fn bind_value<'q>(
        &self,
        query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
        value: &'q Value,
        expected_type: &PgType,
    ) -> Result<sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>> {
        let result = match (value, expected_type) {
            // Null
            (Value::Null, PgType::BigInt) => query.bind::<Option<i64>>(None),
            (Value::Null, PgType::Double) => query.bind::<Option<f64>>(None),
            (Value::Null, PgType::Boolean) => query.bind::<Option<bool>>(None),
            (Value::Null, PgType::Jsonb) => query.bind(Json(Value::Null)),
            (Value::Null, _) => query.bind::<Option<String>>(None),

            // Boolean
            (Value::Bool(b), PgType::Boolean) => query.bind(*b),
            (Value::Bool(b), PgType::Text) => query.bind(b.to_string()),
            (Value::Bool(b), _) => query.bind(b.to_string()),

            // Numbers
            (Value::Number(n), PgType::BigInt) => {
                if let Some(i) = n.as_i64() {
                    query.bind(i)
                } else {
                    query.bind::<Option<i64>>(None)
                }
            }
            (Value::Number(n), PgType::Double) => {
                if let Some(f) = n.as_f64() {
                    query.bind(f)
                } else {
                    query.bind::<Option<f64>>(None)
                }
            }
            (Value::Number(n), PgType::Text) => query.bind(n.to_string()),
            (Value::Number(_), PgType::Jsonb) => query.bind(Json(value)),
            (Value::Number(_), _) => query.bind::<Option<f64>>(None),

            // Strings
            (Value::String(s), PgType::Text) => query.bind(s.as_str()),
            (Value::String(s), PgType::Jsonb) => {
                let json_str = Value::String(s.clone());
                query.bind(Json(json_str))
            }
            (Value::String(s), PgType::BigInt) => {
                if let Ok(i) = s.parse::<i64>() {
                    query.bind(i)
                } else {
                    query.bind::<Option<i64>>(None)
                }
            }
            (Value::String(s), PgType::Double) => {
                if let Ok(f) = s.parse::<f64>() {
                    query.bind(f)
                } else {
                    query.bind::<Option<f64>>(None)
                }
            }
            (Value::String(s), PgType::Boolean) => {
                let b = s.to_lowercase() == "true" || s == "1";
                query.bind(b)
            }

            // Arrays / Objects
            (Value::Array(_), PgType::Jsonb) | (Value::Object(_), PgType::Jsonb) => {
                query.bind(Json(value))
            }
            (Value::Array(_), PgType::Text) | (Value::Object(_), PgType::Text) => {
                query.bind(serde_json::to_string(value).unwrap_or_default())
            }
            (Value::Array(_), _) | (Value::Object(_), _) => {
                query.bind(serde_json::to_string(value).unwrap_or_default())
            }
        };

        Ok(result)
    }
}

#[async_trait]
impl DataWriter for PostgresWriter {
    async fn write_stream(
        &self,
        mut result: QueryResultStream,
        write_mode: WriteMode,
    ) -> Result<()> {
        // Local macro: write one chunk with the chosen mode
        macro_rules! write_chunk {
            ($buf:expr, $schema:expr) => {
                match write_mode {
                    WriteMode::Append => self.insert_batch($buf, $schema).await,
                    WriteMode::Merge => self.merge_batch($buf, $schema).await,
                }
            };
        }

        let mut buf: Vec<serde_json::Value> = Vec::with_capacity(self.batch_size);
        let mut schema: Option<BTreeMap<String, PgType>> = None;

        // Stream → buffer → write in batches
        while let Some(item) = result.data.next().await {
            buf.push(item?);

            if buf.len() >= self.batch_size {
                // Lazily infer/create table schema from current batch
                if schema.is_none() {
                    schema = Some(self.ensure_table(&buf).await?);
                }
                let schema_ref = schema.as_ref().expect("schema just set");
                write_chunk!(&buf, schema_ref)?;
                buf.clear();
            }
        }

        // Flush remainder
        if !buf.is_empty() {
            if schema.is_none() {
                schema = Some(self.ensure_table(&buf).await?);
            }
            let schema_ref = schema.as_ref().expect("schema just set");
            write_chunk!(&buf, schema_ref)?;
        }

        Ok(())
    }

    async fn write(&self, result: QueryResult) -> Result<()> {
        let rows = result
            .data
            .as_array()
            .ok_or_else(|| ApitapError::PipelineError("Expected JSON array".to_string()))?;

        if rows.is_empty() {
            return Ok(());
        }

        let schema = self.ensure_table(rows).await?;

        for chunk in rows.chunks(self.batch_size) {
            self.insert_batch(chunk, &schema).await?;
        }

        Ok(())
    }

    async fn begin(&self) -> Result<()> {
        sqlx::query("BEGIN").execute(&self.pool).await?;
        Ok(())
    }

    async fn commit(&self) -> Result<()> {
        sqlx::query("COMMIT").execute(&self.pool).await?;
        Ok(())
    }

    async fn rollback(&self) -> Result<()> {
        sqlx::query("ROLLBACK").execute(&self.pool).await?;
        Ok(())
    }
}
