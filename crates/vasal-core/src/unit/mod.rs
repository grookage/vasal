//! Unit lifecycle management — install, upgrade, health, remove.

pub mod health;
pub mod install;
pub mod upgrade;

use std::path::{Path, PathBuf};

use vasal_protocol::unit::{ManagedUnit, UnitKind, UnitState};

use crate::state::UnitRow;

pub fn unit_to_row(unit: &ManagedUnit) -> UnitRow {
    UnitRow {
        name: unit.name.clone(),
        kind: match unit.kind {
            UnitKind::Sidecar => "sidecar".into(),
            UnitKind::Package => "package".into(),
        },
        version: unit.version.clone(),
        state: match unit.state {
            UnitState::Running => "running",
            UnitState::Installed => "installed",
            UnitState::Stopped => "stopped",
            UnitState::Absent => "absent",
        }
        .into(),
        health: None,
        health_error: None,
        pid: None,
        socket_path: unit
            .socket_path
            .as_ref()
            .map(|p| p.to_string_lossy().into_owned()),
        config_json: unit.config.as_ref().map(|c| c.to_string()),
        updated_at: crate::state::now_ms(),
    }
}

pub fn socket_path_for(socket_dir: &Path, unit_name: &str) -> PathBuf {
    socket_dir.join(format!("{unit_name}.sock"))
}
