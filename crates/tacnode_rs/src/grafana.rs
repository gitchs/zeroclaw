use reqwest;
use serde::{Deserialize, Serialize};

use super::auth::AuthClient;
use std::{f64, sync::Arc};

pub struct GrafanaAuth {
    pub username: String,
    pub password: String,
}
impl GrafanaAuth {
    pub fn from_env() -> Option<Self> {
        let username = std::env::var("TACNODE_GRAFANA_USERNAME").unwrap_or_default();
        let password = std::env::var("TACNODE_GRAFANA_PASSWORD").unwrap_or_default();
        if username.is_empty() || password.is_empty() {
            return None;
        }
        Some(Self { username, password })
    }
}

pub struct GrafanaClient {
    sources: Vec<GrafanaSource>,
    pub auth: Arc<AuthClient>,
    pub gauth: GrafanaAuth,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct GrafanaSource {
    pub id: i64,
    pub uid: String,
    pub org_id: i64,
    pub name: String,
    #[serde(rename = "type")]
    pub type_: String,
    pub type_name: String,
    pub type_logo_url: String,
    pub access: String,
    pub url: String,
    pub database: String,
}

pub struct GrafanaResponse {
    pub status: String,
}

impl GrafanaClient {
    pub fn new(auth: Arc<AuthClient>, gauth: GrafanaAuth) -> Self {
        Self {
            sources: Vec::new(),
            auth,
            gauth,
        }
    }

    pub async fn init_sources(&mut self) -> anyhow::Result<&Vec<GrafanaSource>> {
        if self.sources.is_empty() {
            let client = reqwest::Client::new();
            let full_url = self.auth.base_url.join("/grafana/api/datasources").unwrap();
            let response = client
                .get(full_url)
                .basic_auth(&self.gauth.username, Some(&self.gauth.password))
                .send()
                .await
                .unwrap();
            self.sources = response.json().await.unwrap();
        }
        Ok(&self.sources)
    }

    pub fn get_prometheus_source(&self) -> anyhow::Result<&GrafanaSource> {
        const PLATFORM_PROMETHEUS_URL: &str = "http://prometheus.monitoring.svc:9090";
        let sources = &self.sources;
        sources
            .iter()
            .find(|s| s.type_ == "prometheus" && s.url == PLATFORM_PROMETHEUS_URL)
            .ok_or(anyhow::anyhow!("platform prometheus not found"))
    }

    pub async fn query(
        &self,
        query: &str,
        from: i64,
        to: i64,
        step: Option<u32>,
    ) -> anyhow::Result<Vec<(i64, f64)>> {
        let source = self.get_prometheus_source().unwrap();
        let client = reqwest::Client::new();
        let mut full_url = self
            .auth
            .base_url
            .join(&format!(
                "/grafana/api/datasources/proxy/{}/api/v1/query_range",
                source.id,
            ))
            .unwrap();
        let step = step.unwrap_or(60);

        full_url
            .query_pairs_mut()
            .append_pair("query", query)
            .append_pair("start", from.to_string().as_str())
            .append_pair("end", to.to_string().as_str())
            .append_pair("step", step.to_string().as_str());

        let builder = client
            .post(full_url)
            .basic_auth(&self.gauth.username, Some(&self.gauth.password));
        let response = builder.send().await.unwrap();
        let text = response.text().await.unwrap();
        let root: serde_json::Value = serde_json::from_str(&text).unwrap();
        let query_status = &root["status"].as_str().unwrap();
        if query_status != &"success" {
            return Err(anyhow::anyhow!("query failed: {}", text));
        }

        let results = &root["data"]["result"];
        let items = results.as_array().unwrap();
        if items.is_empty() {
            return Ok(Vec::new());
        }
        let item = items.first().unwrap();
        let rows = item["values"]
            .as_array()
            .unwrap()
            .iter()
            .map(|point| {
                let ts: i64 = point[0].as_i64().unwrap();
                let val: f64 = point[1].as_str().unwrap().parse().unwrap();
                (ts, val)
            })
            .collect();
        Ok(rows)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DataSyncLatencyStats {
    min: f64,         // min(value)
    max: f64,         // max(value)
    range: f64,       // max(value) - min(value)
    stddev: f64,      // standard deviation
    mean: f64,        // avg(value)
    from: i64,        // start timestamp
    to: i64,          // end timestamp
    step: i64,        // step size in seconds
    miss_count: i32,  // missing data points count
    count: i32,       // number of data points
    trend: f64,       // (mean(last 5 vals) - mean(first 5 vals))/(to - from)
    spike_count: i32, // spike data points count, > mean + 3 * stddev
    cv: f64,          // coefficient of variation, = stddev / mean
                      // < 0.1  : very stable
                      // 0.1-0.3: normal fluctuation
                      // 0.3-0.5: high fluctuation, worth investigating
                      // > 0.5  : unstable, likely problematic
}

impl DataSyncLatencyStats {
    pub fn describe(&self, prefix: &str) -> String {
        format!(
            r##"{}- from/to = {}/{}
{}- range(latency) = {} // max - min
{}- stddev(latency) = {}
{}- min(latency) = {}
{}- max(latency) = {}
{}- mean(latency) = {}
{}- count = {}
{}- miss_count = {} // missing data points count
{}- spike_count = {} // spike data points count
{}- trend = {} // (mean(last 5 vals) - mean(first 5 vals))/(to - from)
{}- cv(latency) = {} // coefficient of variation, = stddev / mean"##,
            prefix,
            self.from,
            self.to,
            prefix,
            self.range,
            prefix,
            self.stddev,
            prefix,
            self.min,
            prefix,
            self.max,
            prefix,
            self.mean,
            prefix,
            self.count,
            prefix,
            self.miss_count,
            prefix,
            self.spike_count,
            prefix,
            self.trend,
            prefix,
            self.cv,
        )
    }

    pub fn from_samples(samples: &Vec<(i64, f64)>) -> Option<Self> {
        if samples.is_empty() {
            return None;
        }
        let mut retval = Self {
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            mean: 0.0,
            range: 0.0,
            stddev: 0.0,
            from: -1,
            to: -1,
            step: -1,
            miss_count: 0,
            count: samples.len() as i32,
            trend: 0.0,
            spike_count: 0,
            cv: 0.0,
        };
        retval.from = samples[0].0;
        retval.to = samples[samples.len() - 1].0;
        retval.min = samples.iter().fold(f64::INFINITY, |acc, &x| acc.min(x.1));
        retval.max = samples
            .iter()
            .fold(f64::NEG_INFINITY, |acc, &x| acc.max(x.1));
        retval.range = retval.max - retval.min;
        if samples.len() > 1 {
            retval.step = samples[1].0 - samples[0].0;
        }
        retval.miss_count = samples
            .windows(2)
            .map(|w| if w[1].0 - w[0].0 > retval.step { 1 } else { 0 })
            .sum();
        retval.mean = samples.iter().map(|v| v.1).sum::<f64>() / samples.len() as f64;
        retval.stddev = samples
            .iter()
            .map(|v| (v.1 - retval.mean).powi(2))
            .sum::<f64>()
            .sqrt();

        if retval.to > retval.from {
            if samples.len() >= 10 {
                // 用前5个点均值和后5个点均值代替首尾单点，增强抗噪音能力
                retval.trend = (samples[..5].iter().map(|v| v.1).sum::<f64>()
                    - samples[samples.len() - 5..]
                        .iter()
                        .map(|v| v.1)
                        .sum::<f64>())
                    / (retval.to - retval.from) as f64;
            } else {
                retval.trend = (samples[samples.len() - 1].1 - samples[0].1)
                    / (retval.to - retval.from) as f64;
            }
        }
        let spike_threshold = retval.mean + 3.0 * retval.stddev;
        retval.spike_count = samples.iter().filter(|v| v.1 > spike_threshold).count() as i32;
        retval.cv = retval.stddev / retval.mean;

        return Some(retval);
    }
}
