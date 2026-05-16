//! `sql-ctrl` — SQL query execution sidecar for the Vasal agent.
//!
//! Executes SQL queries dispatched by the control plane via the Vasal
//! sidecar protocol. Currently supports SQLite via `rusqlite`; the
//! architecture is designed for future extension to MySQL/Postgres.
//!
//! # Actions
//!
//! - `query`    — execute a SQL statement and return rows as JSON.
//! - `exec`     — execute a SQL statement that modifies data (INSERT/UPDATE/DELETE).
//! - `discover` — return schema information (table names, column metadata).
//!
//! # Usage
//!
//! ```bash
//! sql-ctrl --dsn /path/to/database.db /run/vasal/sql-ctrl.sock
//! ```
//!
//! The DSN (data source name) can also be supplied per-request in the
//! task payload, overriding the default.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rusqlite::Connection;
use serde::Deserialize;
use tracing::{debug, info, warn};
use vasal_protocol::sidecar::{HealthResponse, HealthStatus, SubmitResponse};
use vasal_protocol::ProtocolError;
use vasal_sidecar_sdk::{SidecarHandler, SidecarServer};

/// Agent version, injected at compile time.
const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Default socket path.
const DEFAULT_SOCKET: &str = "/run/vasal/sql-ctrl.sock";

// ── Request types ──────────────────────────────────────────────────────────

/// Parameters expected in a `submit` request.
#[derive(Debug, Deserialize)]
struct SqlParams {
    /// Action to perform: "query", "exec", or "discover".
    action: String,
    /// SQLite database path. Overrides the default DSN if provided.
    #[serde(default)]
    dsn: Option<String>,
    /// SQL statement to execute (required for "query" and "exec").
    #[serde(default)]
    sql: Option<String>,
    /// Positional bind parameters for the SQL statement.
    #[serde(default)]
    params: Vec<serde_json::Value>,
}

// ── Handler ────────────────────────────────────────────────────────────────

/// The SQL sidecar handler.
struct SqlCtrl {
    /// Default DSN (database path) from CLI args.
    default_dsn: Option<String>,
    /// Connection pool: one connection per DSN, protected by a mutex.
    /// For SQLite, connections are cheap; we keep one per unique DSN.
    connections: Mutex<Vec<(String, Arc<Mutex<Connection>>)>>,
}

impl SqlCtrl {
    fn new(default_dsn: Option<String>) -> Self {
        Self {
            default_dsn,
            connections: Mutex::new(Vec::new()),
        }
    }

    /// Get or open a connection to the given DSN.
    fn get_connection(&self, dsn: &str) -> Result<Arc<Mutex<Connection>>, ProtocolError> {
        let mut conns = self.connections.lock().unwrap();

        // Check if we already have a connection for this DSN.
        if let Some(entry) = conns.iter().find(|(d, _)| d == dsn) {
            return Ok(Arc::clone(&entry.1));
        }

        // Open a new connection.
        debug!(dsn = %dsn, "opening new SQLite connection");
        let conn = Connection::open(dsn).map_err(|e| {
            ProtocolError::internal_error(format!("failed to open database {dsn}: {e}"))
        })?;
        // Enable WAL for better concurrent access.
        conn.execute_batch("PRAGMA journal_mode = WAL;").ok();
        conn.execute_batch("PRAGMA busy_timeout = 5000;").ok();

        let arc = Arc::new(Mutex::new(conn));
        conns.push((dsn.to_owned(), Arc::clone(&arc)));
        Ok(arc)
    }

    /// Resolve the DSN: use the per-request override or the default.
    fn resolve_dsn(&self, request_dsn: Option<&str>) -> Result<String, ProtocolError> {
        request_dsn
            .map(String::from)
            .or_else(|| self.default_dsn.clone())
            .ok_or_else(|| {
                ProtocolError::invalid_params(
                    "no 'dsn' provided and no default DSN configured",
                )
            })
    }

    /// Execute a read query and return rows as JSON.
    fn execute_query(
        &self,
        dsn: &str,
        sql: &str,
        params: &[serde_json::Value],
    ) -> Result<SubmitResponse, ProtocolError> {
        let conn_arc = self.get_connection(dsn)?;
        let conn = conn_arc.lock().unwrap();

        let mut stmt = conn.prepare(sql).map_err(|e| {
            ProtocolError::internal_error(format!("SQL prepare error: {e}"))
        })?;

        let column_count = stmt.column_count();
        let column_names: Vec<String> = (0..column_count)
            .map(|i| stmt.column_name(i).unwrap_or("?").to_owned())
            .collect();

        let bind_params = to_rusqlite_params(params);
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            bind_params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();

        let rows = stmt
            .query_map(param_refs.as_slice(), |row| {
                let mut obj = serde_json::Map::new();
                for (i, name) in column_names.iter().enumerate() {
                    let value = row_value_at(row, i);
                    obj.insert(name.clone(), value);
                }
                Ok(serde_json::Value::Object(obj))
            })
            .map_err(|e| ProtocolError::internal_error(format!("SQL query error: {e}")))?;

        let mut result_rows = Vec::new();
        for row in rows {
            match row {
                Ok(v) => result_rows.push(v),
                Err(e) => {
                    warn!(error = %e, "error reading row");
                    return Err(ProtocolError::internal_error(format!("row read error: {e}")));
                }
            }
        }

        let output = serde_json::json!({
            "columns": column_names,
            "rows": result_rows,
            "row_count": result_rows.len(),
        });

        Ok(SubmitResponse::Completed {
            stdout: output.to_string(),
            stderr: String::new(),
            truncated: false,
        })
    }

    /// Execute a write statement (INSERT/UPDATE/DELETE) and return affected rows.
    fn execute_write(
        &self,
        dsn: &str,
        sql: &str,
        params: &[serde_json::Value],
    ) -> Result<SubmitResponse, ProtocolError> {
        let conn_arc = self.get_connection(dsn)?;
        let conn = conn_arc.lock().unwrap();

        let bind_params = to_rusqlite_params(params);
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            bind_params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();

        let affected = conn
            .execute(sql, param_refs.as_slice())
            .map_err(|e| ProtocolError::internal_error(format!("SQL exec error: {e}")))?;

        let output = serde_json::json!({
            "affected_rows": affected,
            "last_insert_rowid": conn.last_insert_rowid(),
        });

        Ok(SubmitResponse::Completed {
            stdout: output.to_string(),
            stderr: String::new(),
            truncated: false,
        })
    }

    /// Discover schema: list tables and their columns.
    fn execute_discover(&self, dsn: &str) -> Result<SubmitResponse, ProtocolError> {
        let conn_arc = self.get_connection(dsn)?;
        let conn = conn_arc.lock().unwrap();

        // Get table list.
        let mut stmt = conn
            .prepare(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
            )
            .map_err(|e| ProtocolError::internal_error(format!("discover error: {e}")))?;

        let tables: Vec<String> = stmt
            .query_map([], |row| row.get(0))
            .map_err(|e| ProtocolError::internal_error(format!("discover error: {e}")))?
            .filter_map(|r| r.ok())
            .collect();

        // Get column info for each table.
        let mut table_info = Vec::new();
        for table in &tables {
            let mut info_stmt = conn
                .prepare(&format!("PRAGMA table_info(\"{}\")", table.replace('"', "\"\"")))
                .map_err(|e| ProtocolError::internal_error(format!("table_info error: {e}")))?;

            let columns: Vec<serde_json::Value> = info_stmt
                .query_map([], |row| {
                    let name: String = row.get(1)?;
                    let col_type: String = row.get(2)?;
                    let not_null: bool = row.get(3)?;
                    let pk: bool = row.get(5)?;
                    Ok(serde_json::json!({
                        "name": name,
                        "type": col_type,
                        "not_null": not_null,
                        "primary_key": pk,
                    }))
                })
                .map_err(|e| ProtocolError::internal_error(format!("column info error: {e}")))?
                .filter_map(|r| r.ok())
                .collect();

            table_info.push(serde_json::json!({
                "table": table,
                "columns": columns,
            }));
        }

        let output = serde_json::json!({
            "tables": table_info,
            "table_count": table_info.len(),
            "driver": "sqlite",
        });

        Ok(SubmitResponse::Completed {
            stdout: output.to_string(),
            stderr: String::new(),
            truncated: false,
        })
    }
}

#[async_trait]
impl SidecarHandler for SqlCtrl {
    fn name(&self) -> &str {
        "sql-ctrl"
    }

    /// Health check: verify we can open the default database (if configured).
    async fn health(&self) -> HealthResponse {
        if let Some(dsn) = &self.default_dsn {
            match self.get_connection(dsn) {
                Ok(conn_arc) => {
                    let conn = conn_arc.lock().unwrap();
                    match conn.execute_batch("SELECT 1") {
                        Ok(()) => HealthResponse {
                            status: HealthStatus::Ok,
                            version: Some(VERSION.into()),
                            error: None,
                            metadata: Some(serde_json::json!({
                                "driver": "sqlite",
                                "dsn": dsn,
                            })),
                        },
                        Err(e) => HealthResponse {
                            status: HealthStatus::Unhealthy,
                            version: Some(VERSION.into()),
                            error: Some(format!("database ping failed: {e}")),
                            metadata: None,
                        },
                    }
                }
                Err(e) => HealthResponse {
                    status: HealthStatus::Unhealthy,
                    version: Some(VERSION.into()),
                    error: Some(e.to_string()),
                    metadata: None,
                },
            }
        } else {
            // No default DSN — healthy but idle.
            HealthResponse {
                status: HealthStatus::Ok,
                version: Some(VERSION.into()),
                error: None,
                metadata: Some(serde_json::json!({"driver": "sqlite", "dsn": null})),
            }
        }
    }

    /// Execute a SQL action.
    async fn submit(
        &self,
        params: serde_json::Value,
    ) -> Result<SubmitResponse, ProtocolError> {
        let p: SqlParams = serde_json::from_value(params)
            .map_err(|e| ProtocolError::invalid_params(e.to_string()))?;

        let dsn = self.resolve_dsn(p.dsn.as_deref())?;

        match p.action.as_str() {
            "query" => {
                let sql = p.sql.as_deref().ok_or_else(|| {
                    ProtocolError::invalid_params("missing 'sql' field for query action")
                })?;
                debug!(sql = %sql, dsn = %dsn, "executing query");
                // Run on a blocking thread since rusqlite is synchronous.
                let this_dsn = dsn.clone();
                let this_sql = sql.to_owned();
                let this_params = p.params.clone();
                // We can't move `self` into spawn_blocking, so execute inline.
                // rusqlite operations are typically fast enough for the async runtime.
                self.execute_query(&this_dsn, &this_sql, &this_params)
            }
            "exec" => {
                let sql = p.sql.as_deref().ok_or_else(|| {
                    ProtocolError::invalid_params("missing 'sql' field for exec action")
                })?;
                debug!(sql = %sql, dsn = %dsn, "executing write");
                self.execute_write(&dsn, sql, &p.params)
            }
            "discover" => {
                debug!(dsn = %dsn, "executing discover");
                self.execute_discover(&dsn)
            }
            other => Err(ProtocolError::invalid_params(format!(
                "unknown action: {other} (expected 'query', 'exec', or 'discover')",
            ))),
        }
    }
}

// ── Helpers ────────────────────────────────────────────────────────────────

/// Convert JSON values to rusqlite-compatible bind parameters.
fn to_rusqlite_params(values: &[serde_json::Value]) -> Vec<Box<dyn rusqlite::types::ToSql>> {
    values
        .iter()
        .map(|v| -> Box<dyn rusqlite::types::ToSql> {
            match v {
                serde_json::Value::Null => Box::new(rusqlite::types::Null),
                serde_json::Value::Bool(b) => Box::new(*b),
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        Box::new(i)
                    } else if let Some(f) = n.as_f64() {
                        Box::new(f)
                    } else {
                        Box::new(n.to_string())
                    }
                }
                serde_json::Value::String(s) => Box::new(s.clone()),
                other => Box::new(other.to_string()),
            }
        })
        .collect()
}

/// Extract a value from a rusqlite row at the given index, returning a JSON value.
fn row_value_at(row: &rusqlite::Row<'_>, idx: usize) -> serde_json::Value {
    // Try integer first, then float, then string, then null.
    if let Ok(v) = row.get::<_, i64>(idx) {
        return serde_json::Value::Number(v.into());
    }
    if let Ok(v) = row.get::<_, f64>(idx) {
        return serde_json::Number::from_f64(v)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null);
    }
    if let Ok(v) = row.get::<_, String>(idx) {
        return serde_json::Value::String(v);
    }
    if let Ok(v) = row.get::<_, Vec<u8>>(idx) {
        // Return blobs as base64-ish hex for now.
        return serde_json::Value::String(hex::encode(v));
    }
    serde_json::Value::Null
}

// ── Main ───────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    // Parse CLI: sql-ctrl [--dsn <path>] <socket_path>
    let args: Vec<String> = std::env::args().collect();
    let mut dsn: Option<String> = None;
    let mut socket_path = DEFAULT_SOCKET.to_owned();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--dsn" => {
                i += 1;
                dsn = args.get(i).cloned();
            }
            arg if !arg.starts_with('-') => {
                socket_path = arg.to_owned();
            }
            other => {
                eprintln!("unknown argument: {other}");
                eprintln!("usage: sql-ctrl [--dsn <database_path>] [<socket_path>]");
                std::process::exit(1);
            }
        }
        i += 1;
    }

    info!(
        version = VERSION,
        socket = %socket_path,
        dsn = ?dsn,
        "starting sql-ctrl",
    );

    let handler = SqlCtrl::new(dsn);
    let server = SidecarServer::new(handler, &socket_path);

    // Shut down on SIGTERM or Ctrl-C.
    let shutdown = async {
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("failed to register SIGTERM handler");
        tokio::select! {
            _ = sigterm.recv() => info!("received SIGTERM"),
            _ = tokio::signal::ctrl_c() => info!("received SIGINT"),
        }
    };

    server.run(shutdown).await?;
    Ok(())
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_db() -> (tempfile::TempDir, String) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let dsn = db_path.to_str().unwrap().to_owned();

        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER);
             INSERT INTO users (name, age) VALUES ('alice', 30);
             INSERT INTO users (name, age) VALUES ('bob', 25);
             INSERT INTO users (name, age) VALUES ('charlie', 35);",
        )
        .unwrap();

        (dir, dsn)
    }

    #[test]
    fn query_returns_rows() {
        let (_dir, dsn) = create_test_db();
        let handler = SqlCtrl::new(Some(dsn.clone()));

        let result = handler
            .execute_query(&dsn, "SELECT name, age FROM users ORDER BY name", &[])
            .unwrap();

        match result {
            SubmitResponse::Completed { stdout, .. } => {
                let parsed: serde_json::Value = serde_json::from_str(&stdout).unwrap();
                assert_eq!(parsed["row_count"], 3);
                assert_eq!(parsed["columns"], serde_json::json!(["name", "age"]));
                assert_eq!(parsed["rows"][0]["name"], "alice");
                assert_eq!(parsed["rows"][0]["age"], 30);
                assert_eq!(parsed["rows"][2]["name"], "charlie");
            }
            _ => panic!("expected Completed"),
        }
    }

    #[test]
    fn query_with_bind_params() {
        let (_dir, dsn) = create_test_db();
        let handler = SqlCtrl::new(Some(dsn.clone()));

        let params = vec![serde_json::json!(25)];
        let result = handler
            .execute_query(&dsn, "SELECT name FROM users WHERE age > ? ORDER BY name", &params)
            .unwrap();

        match result {
            SubmitResponse::Completed { stdout, .. } => {
                let parsed: serde_json::Value = serde_json::from_str(&stdout).unwrap();
                assert_eq!(parsed["row_count"], 2);
                assert_eq!(parsed["rows"][0]["name"], "alice");
                assert_eq!(parsed["rows"][1]["name"], "charlie");
            }
            _ => panic!("expected Completed"),
        }
    }

    #[test]
    fn exec_insert_returns_affected() {
        let (_dir, dsn) = create_test_db();
        let handler = SqlCtrl::new(Some(dsn.clone()));

        let params = vec![serde_json::json!("dave"), serde_json::json!(28)];
        let result = handler
            .execute_write(&dsn, "INSERT INTO users (name, age) VALUES (?, ?)", &params)
            .unwrap();

        match result {
            SubmitResponse::Completed { stdout, .. } => {
                let parsed: serde_json::Value = serde_json::from_str(&stdout).unwrap();
                assert_eq!(parsed["affected_rows"], 1);
                assert!(parsed["last_insert_rowid"].as_i64().unwrap() > 0);
            }
            _ => panic!("expected Completed"),
        }

        // Verify the insert persisted.
        let check = handler
            .execute_query(&dsn, "SELECT COUNT(*) as cnt FROM users", &[])
            .unwrap();
        match check {
            SubmitResponse::Completed { stdout, .. } => {
                let parsed: serde_json::Value = serde_json::from_str(&stdout).unwrap();
                assert_eq!(parsed["rows"][0]["cnt"], 4);
            }
            _ => panic!("expected Completed"),
        }
    }

    #[test]
    fn discover_returns_schema() {
        let (_dir, dsn) = create_test_db();
        let handler = SqlCtrl::new(Some(dsn.clone()));

        let result = handler.execute_discover(&dsn).unwrap();

        match result {
            SubmitResponse::Completed { stdout, .. } => {
                let parsed: serde_json::Value = serde_json::from_str(&stdout).unwrap();
                assert_eq!(parsed["table_count"], 1);
                assert_eq!(parsed["driver"], "sqlite");
                let table = &parsed["tables"][0];
                assert_eq!(table["table"], "users");

                let columns = table["columns"].as_array().unwrap();
                assert_eq!(columns.len(), 3);
                assert_eq!(columns[0]["name"], "id");
                assert!(columns[0]["primary_key"].as_bool().unwrap());
                assert_eq!(columns[1]["name"], "name");
                assert_eq!(columns[2]["name"], "age");
            }
            _ => panic!("expected Completed"),
        }
    }

    #[test]
    fn query_invalid_sql_returns_error() {
        let (_dir, dsn) = create_test_db();
        let handler = SqlCtrl::new(Some(dsn.clone()));

        let result = handler.execute_query(&dsn, "SELECT * FROM nonexistent", &[]);
        assert!(result.is_err());
    }

    #[test]
    fn missing_dsn_returns_error() {
        let handler = SqlCtrl::new(None);
        let result = handler.resolve_dsn(None);
        assert!(result.is_err());
    }
}
