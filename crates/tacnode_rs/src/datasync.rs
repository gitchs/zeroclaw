use anyhow::bail;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{
    AuthClient,
    grafana::{GrafanaAuth, GrafanaClient},
};

pub struct DataSyncClient {
    auth: Arc<AuthClient>,
}

impl DataSyncClient {
    pub fn new(auth: Arc<AuthClient>) -> Self {
        Self { auth }
    }

    pub async fn list_jobs(
        &self,
        datacloud: Option<&str>,
        state: Option<&str>,
    ) -> anyhow::Result<Vec<DataSyncJob>> {
        let body = self.auth.get("/ops/api/teleport/jobs").await?;
        let jobs: Vec<DataSyncJob> = serde_json::from_str(&body)?;

        let datacloud = datacloud.unwrap_or("");
        let state = state.unwrap_or_default();
        let jobs = if !datacloud.is_empty() || !state.is_empty() {
            jobs.into_iter()
                .filter(|j| {
                    if datacloud != j.datacloud {
                        return false;
                    }
                    if !state.is_empty() {
                        return state == j.state.as_deref().unwrap_or_default();
                    }
                    true
                })
                .collect()
        } else {
            jobs
        };

        let jobs = jobs
            .into_iter()
            .map(|mut job| {
                if let Some(config) = &mut job.config {
                    let source = &mut config.source;
                    if let Some(connection) = &mut source.connection {
                        connection.as_object_mut().unwrap().remove("password");
                    }
                }
                job
            })
            .collect::<Vec<_>>();
        Ok(jobs)
    }

    pub async fn get_job_latency(
        &self,
        datacloud: &str,
        job_id: &str,
        from: i64,
        to: i64,
        step: Option<u32>,
    ) -> anyhow::Result<Vec<(i64, f64)>> {
        const QUERY_TEMPLATE: &str = r#"min without(instance, instanceId, jobName)
        (
        (
        timestamp(source_event_time{job="teleport",datacloud_id=~"$datacloudId",jobId=~"$jobId"}) - source_event_time{job="teleport",datacloud_id=~"$datacloudId",jobId=~"$jobId"} / 1000
        )
        or
        (
          time() - last_over_time(source_event_time{job="teleport", datacloud_id=~"$datacloudId", jobId=~"$jobId"}[5h]) / 1000
        ))"#;

        let query = QUERY_TEMPLATE
            .replace("$datacloudId", datacloud)
            .replace("$jobId", job_id);
        let gauth: GrafanaAuth = GrafanaAuth::from_env().unwrap();
        let mut client = GrafanaClient::new(self.auth.clone(), gauth);
        let _ = client.init_sources().await?;
        client.query(&query, from, to, step).await
    }

    pub async fn get_job_pod(
        &self,
        datacloud: &str,
        job_id: &str,
    ) -> anyhow::Result<DataSyncJobInstancePod> {
        let response = self
            .auth
            .get(format!("/ops/api/teleport/dataclouds/{}/jobs/{}", datacloud, job_id).as_str())
            .await?;
        let root: DataSyncJobInfo = serde_json::from_str(&response)?;
        if let Some(pod) = root.pod {
            Ok(pod)
        } else {
            bail!("No running_pod found for {}", job_id)
        }
    }

    pub async fn get_meta_mapping(
        &self,
        pod: &DataSyncJobInstancePod,
    ) -> anyhow::Result<Vec<DataSyncJobMetaMapping>> {
        let response = self
            .auth
            .get(
                format!(
                    "/ops/proxy/teleport/{}/{}:9000/summary",
                    pod.name, pod.pod_ip
                )
                .as_str(),
            )
            .await?;
        let root: serde_json::Value = serde_json::from_str(&response)?;
        let tables = root["dashboard"]["tables"].as_array().unwrap();
        for table in tables {
            let name = table["name"].as_str().unwrap().to_string();
            if name == "Meta Mapping" {
                let rows = table["rows"].as_array().unwrap();
                let mapping = rows
                    .iter()
                    .filter(|row| {
                        let sink_type = row["type"].as_str().unwrap().to_string();
                        sink_type == "TABLE"
                    })
                    .map(|row| {
                        let source = row["sourceId"].as_str().unwrap().to_string();
                        let sink = row["sinkId"].as_str().unwrap().to_string();
                        let sink_type = row["type"].as_str().unwrap().to_string();
                        DataSyncJobMetaMapping {
                            source,
                            sink,
                            sink_type,
                        }
                    })
                    .collect::<Vec<_>>();
                return Ok(mapping);
            }
        }
        anyhow::bail!("could not extract `MetaMapping` from `{}`", pod.name)
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct DataSyncJobMetaMapping {
    pub source: String,
    pub sink: String,
    pub sink_type: String,
}

impl DataSyncJobMetaMapping {
    pub fn describe(&self, prefix: &str) -> String {
        let mut sb = String::new();
        sb.push_str(prefix);
        sb.push_str("- src: ");
        sb.push_str(&self.source);
        sb.push_str("\n");
        sb.push_str(prefix);
        sb.push_str("  sink: ");
        sb.push_str(&self.sink);
        sb.push_str("\n");
        sb
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct DataSyncJob {
    pub id: String,
    #[serde(rename = "datacloudId")]
    pub datacloud: String,
    pub datacloud_name: Option<String>,
    pub name: String,
    pub source_type: String,
    pub config: Option<Config>,
    pub creator: Option<String>,
    pub modifier: Option<String>,
    pub state: Option<String>,
    pub gmt_created: i64,
    pub gmt_modified: i64,
    pub spec: Option<String>,
}

impl DataSyncJob {
    pub fn describe(&self) -> String {
        let mut sb = String::new();
        sb.push_str("- id: ");
        sb.push_str(&self.id);
        sb.push_str("\n  name: ");
        sb.push_str(&self.name);
        sb.push_str("\n  source_type: ");
        sb.push_str(&self.source_type);
        sb
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    case_sensitivity_mode: Option<String>,
    data_syncs: Option<serde_json::Value>,
    sink: Sink,
    source: Source,
    spec: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Sink {
    connection: Option<serde_json::Value>,
    encode: Option<serde_json::Value>,
    tunnel_id: Option<serde_json::Value>,
    #[serde(rename = "type")]
    sink_type: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Source {
    connection: Option<serde_json::Value>,
    decoder: Option<serde_json::Value>,
    source_views: Option<serde_json::Value>,
    start_offset: Option<serde_json::Value>,
    tunnel_id: Option<serde_json::Value>,
    #[serde(rename = "type")]
    source_type: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct DataSyncJobInfo {
    pub instances: Option<serde_json::Value>,
    pub job: Option<serde_json::Value>,
    pub pod: Option<DataSyncJobInstancePod>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct DataSyncJobInstancePod {
    pub namespace: String,
    pub name: String,
    pub create_time: i64,
    pub state: String,
    pub start_time: i64,
    pub last_start_time: i64,

    #[serde(rename = "podIP")]
    pub pod_ip: String,
    pub port: i64,
    pub host: String,
    pub uid: String,
    pub images: Vec<String>,
    pub id: String,
    #[serde(rename = "datacloudId")]
    pub datacloud: String,
    pub job_id: String,
    pub job_name: String,
    pub datacloud_name: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils;
    use tracing::debug;

    fn init_env() -> Option<DataSyncClient> {
        let endpoint: String = std::env::var("TACNODE_ENDPOINT").unwrap_or_default();
        let token: String = std::env::var("TACNODE_TOKEN").unwrap_or_default();
        if endpoint.is_empty() || token.is_empty() {
            eprintln!("env TACNODE_ENDPOINT/TACNODE_TOKEN must be set, skip test_list_jobs");
            return None;
        }

        let auth = AuthClient::new(endpoint, token).unwrap();
        let auth = Arc::new(auth);
        let client = DataSyncClient::new(auth);
        Some(client)
    }

    #[tokio::test]
    async fn test_list_jobs() {
        test_utils::init_tracing();
        let client = init_env();
        if client.is_none() {
            return;
        }
        let client = client.unwrap();
        let datacloud = "dc1rqqjhex";
        let state = "RUNNING";
        let jobs = client
            .list_jobs(Some(&datacloud), Some(&state))
            .await
            .unwrap();
        jobs.iter().enumerate().for_each(|(i, job)| {
            debug!(
                r#"# job[{}]
{}
"#,
                i,
                job.describe()
            )
        });
    }

    #[tokio::test]
    async fn test_list_job_meta_mappings() {
        test_utils::init_tracing();
        let client = init_env();
        if client.is_none() {
            return;
        }
        let client = client.unwrap();
        let datacloud = "dc1rqqjhex";
        let job = "tjw4oh9q2k";
        let pod = client.get_job_pod(&datacloud, &job).await.unwrap();
        debug!("job pod: {:?}", pod);
        let mm = client.get_meta_mapping(&pod).await.unwrap();
        debug!("meta mapping: {:?}", mm);
        for m in mm {
            let content = m.describe("  ");
            debug!("content = {}", content);
        }
    }
}
