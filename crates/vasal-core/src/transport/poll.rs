//! HTTP poll transport — task fetch and result reporting (DD-02).
//!
//! The agent periodically GETs pending tasks from the CP's HTTP API and
//! POSTs task results back. Simple, firewall-friendly, works everywhere.

use std::time::Duration;

use async_trait::async_trait;
use tracing::{debug, warn};
use vasal_protocol::task::{Task, TaskChain, TaskResult};

use super::{ReceivedWork, Transport};

/// HTTP poll transport implementation.
pub struct PollTransport {
    endpoint: String,
    http_client: reqwest::Client,
    poll_interval: Duration,
}

impl PollTransport {
    /// Create a new poll transport.
    pub fn new(endpoint: String, http_client: reqwest::Client, poll_interval_sec: u64) -> Self {
        Self {
            endpoint,
            http_client,
            poll_interval: Duration::from_secs(poll_interval_sec),
        }
    }
}

#[async_trait]
impl Transport for PollTransport {
    async fn recv_tasks(&self) -> crate::Result<Vec<ReceivedWork>> {
        // Wait for the poll interval.
        tokio::time::sleep(self.poll_interval).await;

        let url = format!("{}/tasks/pending", self.endpoint);
        let resp = self.http_client.get(&url).send().await?;

        if !resp.status().is_success() {
            return Err(crate::Error::Transport(format!(
                "task poll returned HTTP {}",
                resp.status(),
            )));
        }

        // The CP returns a JSON array of tasks and/or chains.
        let body = resp.text().await?;
        if body.trim().is_empty() || body.trim() == "[]" {
            return Ok(vec![]);
        }

        // Try parsing as an array of tasks.
        let items: Vec<serde_json::Value> = serde_json::from_str(&body)?;
        let mut work = Vec::new();

        for item in items {
            // Check if it's a chain (has "steps" field) or a single task.
            if item.get("steps").is_some() {
                match serde_json::from_value::<TaskChain>(item.clone()) {
                    Ok(chain) => work.push(ReceivedWork::Chain(chain)),
                    Err(e) => {
                        warn!(error = %e, "failed to parse task chain");
                    }
                }
            } else {
                match serde_json::from_value::<Task>(item.clone()) {
                    Ok(task) => work.push(ReceivedWork::Single(task)),
                    Err(e) => {
                        warn!(error = %e, "failed to parse task");
                    }
                }
            }
        }

        debug!(count = work.len(), "received tasks from CP");
        Ok(work)
    }

    async fn send_result(&self, result: &TaskResult) -> crate::Result<()> {
        let url = format!("{}/tasks/result", self.endpoint);
        let resp = self.http_client.post(&url).json(result).send().await?;

        if !resp.status().is_success() {
            return Err(crate::Error::Transport(format!(
                "result report returned HTTP {}",
                resp.status(),
            )));
        }

        debug!(task_id = %result.task_id, "result reported to CP");
        Ok(())
    }
}
