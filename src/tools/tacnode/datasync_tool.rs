use super::super::{Tool, ToolResult};
use anyhow::bail;
use async_trait::async_trait;
use chrono::Timelike;
use serde_json::json;
use serde_json::Value;
use serde_yaml;
use std::sync::Arc;
use tacnode::grafana::{DataSyncLatencyStats, GrafanaAuth, GrafanaClient};
use tacnode::{AuthClient, DataSyncClient};
use tracing::debug;

pub struct DataSyncTool {
    datasync: DataSyncClient,
    grafana: Option<GrafanaClient>,
}

impl DataSyncTool {
    pub fn new() -> Option<Self> {
        let endpoint = std::env::var("TACNODE_ENDPOINT").unwrap();
        let token = std::env::var("TACNODE_TOKEN").unwrap();

        let auth = AuthClient::new(endpoint, token).unwrap();
        let auth = Arc::new(auth);
        let datasync = DataSyncClient::new(auth.clone());
        let gauth = GrafanaAuth::from_env();
        let grafana: Option<GrafanaClient> = if let Some(gauth) = gauth {
            Some(GrafanaClient::new(auth.clone(), gauth))
        } else {
            None
        };
        Some(Self { datasync, grafana })
    }

    async fn list_jobs(&self, args: Value) -> anyhow::Result<ToolResult> {
        debug!("Listing datas jobs, args = {:?}", args);
        let datacloud = args.get("datacloud").unwrap_or_default().as_str();
        let state = args.get("state").unwrap_or_default().as_str();
        let jobs = self.datasync.list_jobs(datacloud, state).await.unwrap();
        let jobs = jobs
            .iter()
            .map(|j| {
                json!({
                    "datacloud": &j.datacloud,
                    "id": &j.id,
                    "name": &j.name,
                    "state": &j.state,
                })
            })
            .collect::<Vec<_>>();
        let wrapper = json!({
            "jobs": jobs,
        });
        debug!("Listing jobs wrapper, wrapper = {:?}", wrapper);
        let body = serde_yaml::to_string(&wrapper)?;
        Ok(ToolResult {
            success: true,
            output: body,
            error: None,
        })
    }

    async fn search_job_tables(&self, args: Value) -> anyhow::Result<ToolResult> {
        debug!("search job tables, args = {:?}", args);
        let datacloud = args.get("datacloud").unwrap().as_str().unwrap();
        let job_id = args.get("job_id").unwrap_or_default().as_str().unwrap();
        let keyword = args
            .get("keyword")
            .unwrap_or_default()
            .as_str()
            .unwrap_or_default();
        let pod = self.datasync.get_job_pod(datacloud, job_id).await?;
        let mm = self.datasync.get_meta_mapping(&pod).await?;
        let tables = mm
            .iter()
            .filter(|m| m.sink.contains(keyword))
            .map(|m| &m.sink)
            .collect::<Vec<_>>();
        match tables.len() {
            0 => Ok(ToolResult {
                success: true,
                output: "tables: []".to_string(),
                error: None,
            }),
            _ => {
                let wrapper = serde_yaml::to_value(json!({
                    "job_id": job_id,
                    "datacloud": datacloud,
                    "tables": tables,
                }))?;
                let body = serde_yaml::to_string(&wrapper)?;
                Ok(ToolResult {
                    success: true,
                    output: body,
                    error: None,
                })
            }
        }
    }

    async fn get_job_tables(&self, args: Value) -> anyhow::Result<ToolResult> {
        debug!("Getting datas job tables: {:?}", args);
        let datacloud = args.get("datacloud").unwrap().as_str().unwrap();
        let job_id = args.get("job_id").unwrap_or_default().as_str().unwrap();
        let pod = self.datasync.get_job_pod(datacloud, job_id).await?;
        let mm = self.datasync.get_meta_mapping(&pod).await?;
        let body = serde_yaml::to_string(&mm)?;
        Ok(ToolResult {
            success: true,
            output: body,
            error: None,
        })
    }
    async fn get_job_latency(&self, args: Value) -> anyhow::Result<ToolResult> {
        debug!("Getting job latency: {:?}", args);
        let datacloud = args.get("datacloud").unwrap().as_str().unwrap();
        let job_id = args.get("job_id").unwrap_or_default().as_str().unwrap();
        let now = chrono::Utc::now()
            .with_second(0)
            .unwrap()
            .with_nanosecond(0)
            .unwrap();
        let from = now - chrono::Duration::hours(6);

        let samples = self
            .datasync
            .get_job_latency(
                datacloud,
                job_id,
                from.timestamp(),
                now.timestamp(),
                Some(300),
            )
            .await?;
        let latency = DataSyncLatencyStats::from_samples(&samples);
        if let Some(latency) = latency {
            let body = serde_yaml::to_string(&latency)?;
            Ok(ToolResult {
                success: true,
                output: body,
                error: None,
            })
        } else {
            Ok(ToolResult {
                success: true,
                output: "job has no latency stat in last 6 hours".into(),
                error: None,
            })
        }
    }
}

#[async_trait]
impl Tool for DataSyncTool {
    fn name(&self) -> &str {
        "datasync_tool"
    }
    fn description(&self) -> &str {
        "Query datasync task metadata.
Actions:
- list-jobs: list jobs, optionally filter by 'datacloud' (namespace) and 'state' (RUNNING or PAUSED)
- get-job-latency: query job latency info, requires 'datacloud' and 'job_id'
- search-sink-tables: only work for running job, filter job sink tables with given keyword, requires 'datacloud' and 'job_id', 'keyword' is optional

Always pass filter conditions as individual fields, never as a combined query string.
"
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["list-jobs", "get-job-latency", "search-sink-tables"]
                },
                "datacloud": {
                    "type": "string",
                    "description": "Datacloud namespace."
                },
                "state": {
                    "type": "string",
                    "description": "Optional for list-jobs",
                    "enum": ["RUNNING", "PAUSED"]
                },
                "job_id": {
                    "type": "string",
                    "description": "Job unique identifier. Required for get-job-latency and search-sink-tables."
                },
                "keyword": {
                    "type": "string",
                    "description": "Filter job tables by give keyword.",
                },
            },
            "required": ["action"]
        })
    }

    async fn execute(&self, args: Value) -> anyhow::Result<ToolResult> {
        let action = args.get("action").unwrap().as_str().unwrap();
        match action.as_ref() {
            "list-jobs" => self.list_jobs(args).await,
            "get-job-latency" => self.get_job_latency(args).await,
            "search-sink-tables" => self.search_job_tables(args).await,
            _ => bail!("Unknown action"),
        }
    }
}
