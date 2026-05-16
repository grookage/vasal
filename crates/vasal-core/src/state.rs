//! SQLite state store — managed units, task journal, and audit log (DD-09).
//!
//! The store uses WAL mode for concurrent readers and a single writer.
//! All operations are synchronous (rusqlite) and should be called from
//! `spawn_blocking` when on the async runtime.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use rusqlite::{params, Connection, OptionalExtension};
use tracing::info;

/// Persistent state store backed by SQLite.
///
/// Thread-safe via interior `Mutex<Connection>`. Clone is cheap (Arc).
#[derive(Clone)]
pub struct StateStore {
    conn: Arc<Mutex<Connection>>,
    data_dir: PathBuf,
}

impl StateStore {
    /// Open (or create) the state database at `<data_dir>/state.db`.
    pub fn open(data_dir: &Path) -> crate::Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        let db_path = data_dir.join("state.db");
        let conn = Connection::open(&db_path)?;

        // Enable WAL mode for better concurrent read performance.
        conn.execute_batch("PRAGMA journal_mode = WAL;")?;
        conn.execute_batch("PRAGMA busy_timeout = 5000;")?;

        let store = Self {
            conn: Arc::new(Mutex::new(conn)),
            data_dir: data_dir.to_owned(),
        };
        store.run_migrations()?;

        info!(path = %db_path.display(), "state store opened");
        Ok(store)
    }

    /// Open an in-memory database (for testing).
    #[cfg(test)]
    pub fn open_in_memory() -> crate::Result<Self> {
        let conn = Connection::open_in_memory()?;
        let store = Self {
            conn: Arc::new(Mutex::new(conn)),
            data_dir: PathBuf::from("/tmp/vasal-test"),
        };
        store.run_migrations()?;
        Ok(store)
    }

    /// Return the data directory used when opening this store.
    pub fn data_dir_or_default(&self) -> PathBuf {
        self.data_dir.clone()
    }

    fn run_migrations(&self) -> crate::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS units (
                name         TEXT PRIMARY KEY,
                kind         TEXT NOT NULL,
                version      TEXT NOT NULL,
                state        TEXT NOT NULL DEFAULT 'absent',
                health       TEXT,
                health_error TEXT,
                pid          INTEGER,
                socket_path  TEXT,
                config_json  TEXT,
                updated_at   INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS task_journal (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id     TEXT NOT NULL,
                chain_id    TEXT,
                step_index  INTEGER,
                status      TEXT NOT NULL,
                exit_code   INTEGER,
                stdout      TEXT NOT NULL DEFAULT '',
                stderr      TEXT NOT NULL DEFAULT '',
                duration_ms INTEGER NOT NULL,
                created_at  INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS audit_log (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp    INTEGER NOT NULL,
                event_type   TEXT NOT NULL,
                task_id      TEXT,
                detail_json  TEXT NOT NULL DEFAULT '{}',
                forwarded    INTEGER NOT NULL DEFAULT 0
            );
            ",
        )?;
        Ok(())
    }

    // ── Units ──────────────────────────────────────────────────────────

    /// Insert or update a managed unit.
    pub fn upsert_unit(&self, unit: &UnitRow) -> crate::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO units (name, kind, version, state, health, health_error, pid, socket_path, config_json, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
             ON CONFLICT(name) DO UPDATE SET
                 kind=excluded.kind, version=excluded.version, state=excluded.state,
                 health=excluded.health, health_error=excluded.health_error, pid=excluded.pid,
                 socket_path=excluded.socket_path, config_json=excluded.config_json,
                 updated_at=excluded.updated_at",
            params![
                unit.name, unit.kind, unit.version, unit.state,
                unit.health, unit.health_error, unit.pid,
                unit.socket_path, unit.config_json, unit.updated_at,
            ],
        )?;
        Ok(())
    }

    /// Look up a unit by name.
    pub fn get_unit(&self, name: &str) -> crate::Result<Option<UnitRow>> {
        let conn = self.conn.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT name, kind, version, state, health, health_error, pid, socket_path, config_json, updated_at
                 FROM units WHERE name = ?1",
                params![name],
                UnitRow::from_row,
            )
            .optional()?;
        Ok(row)
    }

    /// List all managed units.
    pub fn list_units(&self) -> crate::Result<Vec<UnitRow>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT name, kind, version, state, health, health_error, pid, socket_path, config_json, updated_at
             FROM units ORDER BY name",
        )?;
        let rows = stmt
            .query_map([], UnitRow::from_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Remove a unit by name.
    pub fn remove_unit(&self, name: &str) -> crate::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM units WHERE name = ?1", params![name])?;
        Ok(())
    }

    // ── Task Journal ───────────────────────────────────────────────────

    /// Record a task execution result.
    pub fn record_task_result(&self, r: &TaskResultRow) -> crate::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO task_journal (task_id, chain_id, step_index, status, exit_code, stdout, stderr, duration_ms, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                r.task_id, r.chain_id, r.step_index, r.status,
                r.exit_code, r.stdout, r.stderr, r.duration_ms, r.created_at,
            ],
        )?;
        Ok(())
    }

    /// Prune the task journal to keep at most `keep` entries.
    pub fn prune_journal(&self, keep: usize) -> crate::Result<usize> {
        let conn = self.conn.lock().unwrap();
        let deleted = conn.execute(
            "DELETE FROM task_journal WHERE id NOT IN (
                SELECT id FROM task_journal ORDER BY id DESC LIMIT ?1
            )",
            params![keep as i64],
        )?;
        Ok(deleted)
    }

    // ── Audit Log ──────────────────────────────────────────────────────

    /// Append an audit event.
    pub fn append_audit(&self, event: &AuditRow) -> crate::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO audit_log (timestamp, event_type, task_id, detail_json, forwarded)
             VALUES (?1, ?2, ?3, ?4, 0)",
            params![event.timestamp, event.event_type, event.task_id, event.detail_json],
        )?;
        Ok(())
    }

    /// Fetch up to `limit` un-forwarded audit events.
    pub fn pending_audit_events(&self, limit: usize) -> crate::Result<Vec<AuditRow>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, timestamp, event_type, task_id, detail_json, forwarded
             FROM audit_log WHERE forwarded = 0 ORDER BY id LIMIT ?1",
        )?;
        let rows = stmt
            .query_map(params![limit as i64], AuditRow::from_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Mark audit events as forwarded.
    pub fn mark_forwarded(&self, ids: &[i64]) -> crate::Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let conn = self.conn.lock().unwrap();
        // Use a simple loop — batch sizes are small (audit.batch_size).
        let mut stmt = conn.prepare("UPDATE audit_log SET forwarded = 1 WHERE id = ?1")?;
        for id in ids {
            stmt.execute(params![id])?;
        }
        Ok(())
    }
}

// ── Row types ──────────────────────────────────────────────────────────────

/// Row representation of a managed unit in SQLite.
#[derive(Debug, Clone)]
pub struct UnitRow {
    pub name: String,
    pub kind: String,
    pub version: String,
    pub state: String,
    pub health: Option<String>,
    pub health_error: Option<String>,
    pub pid: Option<u32>,
    pub socket_path: Option<String>,
    pub config_json: Option<String>,
    pub updated_at: i64,
}

impl UnitRow {
    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            name: row.get(0)?,
            kind: row.get(1)?,
            version: row.get(2)?,
            state: row.get(3)?,
            health: row.get(4)?,
            health_error: row.get(5)?,
            pid: row.get::<_, Option<i64>>(6)?.map(|v| v as u32),
            socket_path: row.get(7)?,
            config_json: row.get(8)?,
            updated_at: row.get(9)?,
        })
    }
}

/// Row representation of a task result in the journal.
#[derive(Debug, Clone)]
pub struct TaskResultRow {
    pub task_id: String,
    pub chain_id: Option<String>,
    pub step_index: Option<i32>,
    pub status: String,
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    pub duration_ms: i64,
    pub created_at: i64,
}

/// Row representation of an audit event.
#[derive(Debug, Clone)]
pub struct AuditRow {
    pub id: Option<i64>,
    pub timestamp: i64,
    pub event_type: String,
    pub task_id: Option<String>,
    pub detail_json: String,
}

impl AuditRow {
    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            id: Some(row.get(0)?),
            timestamp: row.get(1)?,
            event_type: row.get(2)?,
            task_id: row.get(3)?,
            detail_json: row.get(4)?,
        })
    }
}

/// Return current Unix epoch in milliseconds.
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_crud() {
        let store = StateStore::open_in_memory().unwrap();

        let unit = UnitRow {
            name: "echo-ctrl".into(),
            kind: "sidecar".into(),
            version: "0.1.0".into(),
            state: "running".into(),
            health: Some("ok".into()),
            health_error: None,
            pid: Some(1234),
            socket_path: Some("/run/vasal/echo-ctrl.sock".into()),
            config_json: None,
            updated_at: now_ms(),
        };

        store.upsert_unit(&unit).unwrap();
        let fetched = store.get_unit("echo-ctrl").unwrap().unwrap();
        assert_eq!(fetched.version, "0.1.0");
        assert_eq!(fetched.pid, Some(1234));

        let all = store.list_units().unwrap();
        assert_eq!(all.len(), 1);

        store.remove_unit("echo-ctrl").unwrap();
        assert!(store.get_unit("echo-ctrl").unwrap().is_none());
    }

    #[test]
    fn task_journal_and_prune() {
        let store = StateStore::open_in_memory().unwrap();

        for i in 0..10 {
            store
                .record_task_result(&TaskResultRow {
                    task_id: format!("task-{i}"),
                    chain_id: None,
                    step_index: None,
                    status: "success".into(),
                    exit_code: Some(0),
                    stdout: format!("output-{i}"),
                    stderr: String::new(),
                    duration_ms: 100,
                    created_at: now_ms(),
                })
                .unwrap();
        }

        let deleted = store.prune_journal(5).unwrap();
        assert_eq!(deleted, 5);
    }

    #[test]
    fn audit_log_lifecycle() {
        let store = StateStore::open_in_memory().unwrap();

        store
            .append_audit(&AuditRow {
                id: None,
                timestamp: now_ms(),
                event_type: "task.started".into(),
                task_id: Some("t1".into()),
                detail_json: "{}".into(),
            })
            .unwrap();

        let pending = store.pending_audit_events(10).unwrap();
        assert_eq!(pending.len(), 1);

        let ids: Vec<i64> = pending.iter().filter_map(|r| r.id).collect();
        store.mark_forwarded(&ids).unwrap();

        let pending_after = store.pending_audit_events(10).unwrap();
        assert!(pending_after.is_empty());
    }
}
