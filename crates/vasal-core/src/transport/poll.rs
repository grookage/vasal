//! HTTP poll transport for task fetch and result reporting.

use std::time::Duration;

use async_trait::async_trait;
use tracing::{debug, warn};
use vasal_protocol::task::{Task, TaskChain, TaskResult};

use super::{ReceivedWork, Transport};

pub struct PollTransport {
    endpoint: String,
    http_client: reqwest::Client,
    poll_interval: Duration,
}

impl PollTransport {
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
        tokio::time::sleep(self.poll_interval).await;

        let url = format!("{}/tasks/pending", self.endpoint);
        let resp = self.http_client.get(&url).send().await?;

        if !resp.status().is_success() {
            return Err(crate::Error::Transport(format!(
                "task poll returned HTTP {}",
                resp.status(),
            )));
        }

        let body = resp.text().await?;
        if body.trim().is_empty() || body.trim() == "[]" {
            return Ok(vec![]);
        }

        let items: Vec<serde_json::Value> = serde_json::from_str(&body)?;
        let mut work = Vec::new();

        for item in items {
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
