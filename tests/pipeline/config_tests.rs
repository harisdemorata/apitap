use apitap::http::fetcher::Pagination;
use apitap::pipeline::{Config, PostgresAuth, Retry, Target};

#[test]
fn test_config_source_indexing() {
    let config_yaml = r#"
sources:
  - name: api1
    url: https://api.example.com/users
    table_destination_name: users
    retry:
      max_attempts: 3
      max_delay_secs: 60
      min_delay_secs: 1
  - name: api2
    url: https://api.example.com/posts
    table_destination_name: posts
    retry:
      max_attempts: 3
      max_delay_secs: 60
      min_delay_secs: 1
targets:
  - type: postgres
    name: pg_sink
    host: localhost
    port: 5432
    database: testdb
    auth:
      username: testuser
      password: testpass
"#;

    let config: Config = serde_yaml::from_str(config_yaml).unwrap();

    // Test source retrieval by name
    assert!(config.source("api1").is_some());
    assert!(config.source("api2").is_some());
    assert!(config.source("nonexistent").is_none());

    let retrieved = config.source("api1").unwrap();
    assert_eq!(retrieved.name, "api1");
    assert_eq!(retrieved.url, "https://api.example.com/users");
}

#[test]
fn test_config_target_indexing() {
    let config_yaml = r#"
sources: []
targets:
  - type: postgres
    name: pg_sink
    host: localhost
    port: 5432
    database: testdb
    auth:
      username: testuser
      password: testpass
"#;

    let config: Config = serde_yaml::from_str(config_yaml).unwrap();

    // Test target retrieval
    assert!(config.target("pg_sink").is_some());
    assert!(config.target("nonexistent").is_none());

    let target = config.target("pg_sink").unwrap();
    match target {
        Target::Postgres(pg) => {
            assert_eq!(pg.name, "pg_sink");
            assert_eq!(pg.host, "localhost");
            assert_eq!(pg.port, 5432);
            assert_eq!(pg.database, "testdb");
        }
    }
}

#[test]
fn test_config_duplicate_source_names() {
    let config_yaml = r#"
sources:
  - name: duplicate
    url: https://api1.example.com
    retry:
      max_attempts: 3
      max_delay_secs: 60
      min_delay_secs: 1
  - name: duplicate
    url: https://api2.example.com
    retry:
      max_attempts: 3
      max_delay_secs: 60
      min_delay_secs: 1
targets: []
"#;

    let result: Result<Config, _> = serde_yaml::from_str(config_yaml);
    assert!(result.is_err());
}

#[test]
fn test_config_duplicate_target_names() {
    let config_yaml = r#"
sources: []
targets:
  - type: postgres
    name: duplicate
    host: localhost
    database: db1
    auth:
      username: user1
      password: pass1
  - type: postgres
    name: duplicate
    host: localhost
    database: db2
    auth:
      username: user2
      password: pass2
"#;

    let result: Result<Config, _> = serde_yaml::from_str(config_yaml);
    assert!(result.is_err());
}

#[test]
fn test_postgres_auth_inline_credentials() {
    let auth = PostgresAuth {
        username: Some("testuser".to_string()),
        password: Some("testpass".to_string()),
        username_env: None,
        password_env: None,
    };

    assert!(auth.username.is_some());
    assert!(auth.password.is_some());
    assert!(auth.username_env.is_none());
    assert!(auth.password_env.is_none());
}

#[test]
fn test_postgres_auth_env_credentials() {
    let auth = PostgresAuth {
        username: None,
        password: None,
        username_env: Some("DB_USER".to_string()),
        password_env: Some("DB_PASS".to_string()),
    };

    assert!(auth.username.is_none());
    assert!(auth.password.is_none());
    assert!(auth.username_env.is_some());
    assert!(auth.password_env.is_some());
}

#[test]
fn test_postgres_sink_default_port() {
    let config_yaml = r#"
sources: []
targets:
  - type: postgres
    name: pg_sink
    host: localhost
    database: testdb
    auth:
      username: testuser
      password: testpass
"#;

    let config: Config = serde_yaml::from_str(config_yaml).unwrap();
    let target = config.target("pg_sink").unwrap();

    match target {
        Target::Postgres(pg) => {
            assert_eq!(pg.port, 5432); // default port
        }
    }
}

#[test]
fn test_postgres_sink_custom_port() {
    let config_yaml = r#"
sources: []
targets:
  - type: postgres
    name: pg_sink
    host: localhost
    port: 5433
    database: testdb
    auth:
      username: testuser
      password: testpass
"#;

    let config: Config = serde_yaml::from_str(config_yaml).unwrap();
    let target = config.target("pg_sink").unwrap();

    match target {
        Target::Postgres(pg) => {
            assert_eq!(pg.port, 5433);
        }
    }
}

#[test]
fn test_retry_configuration() {
    let retry = Retry {
        max_attempts: 5,
        max_delay_secs: 120,
        min_delay_secs: 2,
    };

    assert_eq!(retry.max_attempts, 5);
    assert_eq!(retry.max_delay_secs, 120);
    assert_eq!(retry.min_delay_secs, 2);
}

#[test]
fn test_source_with_pagination() {
    let config_yaml = r#"
sources:
  - name: api_with_pagination
    url: https://api.example.com/data
    pagination:
      kind: limit_offset
      limit_param: limit
      offset_param: offset
    retry:
      max_attempts: 3
      max_delay_secs: 60
      min_delay_secs: 1
targets: []
"#;

    let config: Config = serde_yaml::from_str(config_yaml).unwrap();
    let source = config.source("api_with_pagination").unwrap();

    assert!(source.pagination.is_some());
    match source.pagination.as_ref().unwrap() {
        Pagination::LimitOffset {
            limit_param,
            offset_param,
        } => {
            assert_eq!(limit_param, "limit");
            assert_eq!(offset_param, "offset");
        }
        _ => panic!("Expected LimitOffset pagination"),
    }
}

#[test]
fn test_source_without_pagination() {
    let config_yaml = r#"
sources:
  - name: simple_api
    url: https://api.example.com/data
    retry:
      max_attempts: 3
      max_delay_secs: 60
      min_delay_secs: 1
targets: []
"#;

    let config: Config = serde_yaml::from_str(config_yaml).unwrap();
    let source = config.source("simple_api").unwrap();

    assert!(source.pagination.is_none());
}

#[test]
fn test_source_with_table_destination() {
    let config_yaml = r#"
sources:
  - name: api1
    url: https://api.example.com/data
    table_destination_name: custom_table
    retry:
      max_attempts: 3
      max_delay_secs: 60
      min_delay_secs: 1
targets: []
"#;

    let config: Config = serde_yaml::from_str(config_yaml).unwrap();
    let source = config.source("api1").unwrap();

    assert_eq!(
        source.table_destination_name,
        Some("custom_table".to_string())
    );
}

#[test]
fn test_pagination_page_number() {
    let config_yaml = r#"
sources:
  - name: api1
    url: https://api.example.com/data
    pagination:
      kind: page_number
      page_param: page
      per_page_param: per_page
    retry:
      max_attempts: 3
      max_delay_secs: 60
      min_delay_secs: 1
targets: []
"#;

    let config: Config = serde_yaml::from_str(config_yaml).unwrap();
    let source = config.source("api1").unwrap();

    match source.pagination.as_ref().unwrap() {
        Pagination::PageNumber {
            page_param,
            per_page_param,
        } => {
            assert_eq!(page_param, "page");
            assert_eq!(per_page_param, "per_page");
        }
        _ => panic!("Expected PageNumber pagination"),
    }
}

#[test]
fn test_pagination_cursor() {
    let config_yaml = r#"
sources:
  - name: api1
    url: https://api.example.com/data
    pagination:
      kind: cursor
      cursor_param: cursor
      page_size_param: size
    retry:
      max_attempts: 3
      max_delay_secs: 60
      min_delay_secs: 1
targets: []
"#;

    let config: Config = serde_yaml::from_str(config_yaml).unwrap();
    let source = config.source("api1").unwrap();

    match source.pagination.as_ref().unwrap() {
        Pagination::Cursor {
            cursor_param,
            page_size_param,
        } => {
            assert_eq!(cursor_param, "cursor");
            assert_eq!(page_size_param, &Some("size".to_string()));
        }
        _ => panic!("Expected Cursor pagination"),
    }
}

#[test]
fn test_config_serialization_deserialization() {
    let config_yaml = r#"
sources:
  - name: api1
    url: https://api.example.com/data
    retry:
      max_attempts: 3
      max_delay_secs: 60
      min_delay_secs: 1
targets:
  - type: postgres
    name: pg_sink
    host: localhost
    port: 5432
    database: testdb
    auth:
      username: testuser
      password: testpass
"#;

    let config: Config = serde_yaml::from_str(config_yaml).unwrap();

    // Serialize back to YAML
    let serialized = serde_yaml::to_string(&config).unwrap();

    // Deserialize again
    let config2: Config = serde_yaml::from_str(&serialized).unwrap();

    assert_eq!(config.sources.len(), config2.sources.len());
    assert_eq!(config.targets.len(), config2.targets.len());
}
