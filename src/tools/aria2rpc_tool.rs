use super::{Tool, ToolResult};
use anyhow::{anyhow, bail};
use async_trait::async_trait;
use serde_json::{json, Value};

use tracing::debug;
pub struct Aria2RPCTool {
    endpoint: String,
    token: String,
}

fn file_basename(path: &str) -> &str {
    std::path::Path::new(path)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or_default()
}

impl Aria2RPCTool {
    pub fn new() -> Option<Self> {
        let endpoint = std::env::var("ZEROCLAW_ARIA2RPC_ENDPOINT").unwrap_or_default();
        let token = std::env::var("ZEROCLAW_ARIA2RPC_TOKEN").unwrap_or_default();
        if endpoint.is_empty() || token.is_empty() {
            None
        } else {
            Some(Self {
                endpoint,
                token: format!("token:{}", token),
            })
        }
    }

    async fn add_uri(&self, args: &Value) -> anyhow::Result<ToolResult> {
        let params = args
            .get("params")
            .ok_or_else(|| anyhow!("Missing params for add"))?;
        let uris: Vec<String> = params
            .get("uris")
            .ok_or_else(|| anyhow!("Missing uris"))?
            .as_array()
            .ok_or_else(|| anyhow!("uris must be an array"))?
            .iter()
            .map(|x| {
                let s = x.as_str().ok_or_else(|| anyhow!("uri must be a string"))?;
                Ok(
                    if s.len() == 40 && s.chars().all(|c| c.is_ascii_hexdigit()) {
                        format!("magnet:?xt=urn:btih:{}", s)
                    } else {
                        s.to_string()
                    },
                )
            })
            .collect::<anyhow::Result<Vec<String>>>()?;

        let data = json!({
            "jsonrpc": "2.0",
            "id": uuid::Uuid::new_v4().to_string(),
            "method": "aria2.addUri",
            "params": [
                self.token.clone(),
                uris,
            ],
        });
        let client = reqwest::Client::new();
        let response = client.post(&self.endpoint).json(&data).send().await?;
        let payload: Value = response.json().await?;
        let oid = payload
            .get("result")
            .ok_or_else(|| anyhow!("Missing result"))?
            .as_str()
            .ok_or_else(|| anyhow!("Missing result"))?;
        Ok(ToolResult {
            success: true,
            output: format!("oid: {}", oid),
            error: None,
        })
    }

    fn format_task(task: &Value) -> Option<Value> {
        let gid = task["gid"].as_str()?;
        let title = task["bittorrent"]["info"]["name"].as_str().unwrap_or("");
        let title = if title.is_empty() {
            let path0 = task["files"][0]["path"].as_str().unwrap_or("");
            std::path::Path::new(path0)
                .file_name()
                .unwrap_or_default()
                .to_str()
                .unwrap_or_default()
        } else {
            title
        };
        let status = task["status"].as_str()?;
        if status == "complete" && title.starts_with("[METADATA]") {
            return None;
        }
        let error = task["errorMessage"].as_str().unwrap_or_default();
        Some(json!({
                "gid": gid,
                "title": title,
                "status": status,
                "error": error,
        }))
    }

    async fn tell_downloads(&self, _args: &Value) -> anyhow::Result<ToolResult> {
        let data = json!({
            "jsonrpc": "2.0",
            "id": uuid::Uuid::new_v4().to_string(),
            "method": "system.multicall",
            "params": [[
                {
                    "methodName": "aria2.tellActive",
                    "params": [self.token.clone(),]
                },
                {
                    "methodName": "aria2.tellWaiting",
                    "params": [self.token.clone(), 0, 128]
                },
                {
                    "methodName": "aria2.tellStopped",
                    "params": [self.token.clone(), 0, 128]
                }
            ]]
        });

        let client = reqwest::Client::new();
        let response = client.post(&self.endpoint).json(&data).send().await?;
        let payload: Value = response.json().await?;
        let result = payload
            .get("result")
            .ok_or_else(|| anyhow!("Missing result"))?
            .as_array()
            .ok_or_else(|| anyhow!("result must be an array"))?;
        if result.len() != 3 {
            bail!("wrong number of results, {} != 3", result.len());
        }

        let mut task_infos: Vec<Value> = vec![];
        for task_collection in result {
            let tasks = task_collection[0]
                .as_array()
                .ok_or_else(|| anyhow!("Missing tasks"))?;
            for task in tasks {
                if let Some(info) = Self::format_task(task) {
                    task_infos.push(info);
                }
            }
        }
        let output = serde_yaml::to_string(&json!({
            "tasks": task_infos,
        }))?;

        Ok(ToolResult {
            success: true,
            output,
            error: None,
        })
    }

    async fn get_files(&self, args: &Value) -> anyhow::Result<ToolResult> {
        let gid = args["params"]["gid"]
            .as_str()
            .ok_or_else(|| anyhow!("Missing gid"))?;
        let data = json!({
            "jsonrpc": "2.0",
            "id": uuid::Uuid::new_v4().to_string(),
            "method": "aria2.getFiles",
            "params": [self.token.clone(), gid],
        });
        let client = reqwest::Client::new();
        let response = client.post(&self.endpoint).json(&data).send().await?;
        let payload: Value = response.json().await?;
        let files = payload["result"]
            .as_array()
            .ok_or_else(|| anyhow!("Missing result"))?;
        let infos = files
            .iter()
            .filter(|f| {
                if f["selected"].as_str().unwrap_or_default() != "true" {
                    return false;
                }
                let basename = file_basename(f["path"].as_str().unwrap_or_default());
                !basename.starts_with("[METADATA]")
            })
            .map(|f| {
                let index: i64 = f["index"]
                    .as_str()
                    .unwrap_or("0")
                    .parse()
                    .unwrap_or_default();
                let completed_length: i64 = f["completedLength"]
                    .as_str()
                    .unwrap_or("0")
                    .parse()
                    .unwrap_or_default();
                let length: i64 = f["length"]
                    .as_str()
                    .unwrap_or("0")
                    .parse()
                    .unwrap_or_default();
                let basename = file_basename(f["path"].as_str().unwrap_or_default());
                let progress = if length > 0 {
                    Some(completed_length as f64 / length as f64)
                } else {
                    None
                };

                json!({
                    "index": index,
                    "completedLength": completed_length,
                    "length": length,
                    "basename": basename,
                    "progress": progress,
                })
            })
            .collect::<Vec<_>>();
        let output = serde_yaml::to_string(&json!({
            "gid": gid,
            "files": infos,
        }))?;
        Ok(ToolResult {
            success: true,
            output,
            error: None,
        })
    }

    async fn tell_status(&self, args: &Value) -> anyhow::Result<ToolResult> {
        let gid = args["params"]["gid"].as_str().unwrap();
        let data = json!({
            "jsonrpc": "2.0",
            "id": uuid::Uuid::new_v4().to_string(),
            "method": "aria2.tellStatus",
            "params": [
                self.token.clone(),
                &gid,
            ],
        });
        let response = reqwest::Client::new();
        let response = response.post(&self.endpoint).json(&data).send().await?;
        let payload: Value = response.json().await?;
        let output = serde_yaml::to_string(&payload)?;
        Ok(ToolResult {
            success: true,
            output,
            error: None,
        })
    }

    async fn pause_task(&self, args: &Value) -> anyhow::Result<ToolResult> {
        let all = args["params"]["all"].as_bool().unwrap_or_default();
        let gid = args["params"]["gid"].as_str().unwrap_or_default();
        let force = args["params"]["force"].as_bool().unwrap_or_default();
        let data = match (all, gid.is_empty()) {
            (true, false) => bail!("`all` and `gid` are mutually exclusive"),
            (false, true) => bail!("must provide either `all` or `gid`"),
            (true, true) => {
                let method = match force {
                    true => "aria2.forcePauseAll",
                    false => "aria2.pauseAll",
                };
                json!({
                    "jsonrpc": "2.0",
                    "id": uuid::Uuid::new_v4().to_string(),
                    "method": method,
                    "params": [self.token.clone()],
                })
            }
            (false, false) => {
                let method = match force {
                    true => "aria2.forcePause",
                    false => "aria2.pause",
                };
                json!({
                        "jsonrpc": "2.0",
                        "id": uuid::Uuid::new_v4().to_string(),
                        "method": method,
                        "params": [
                        self.token.clone(),
                        gid,
                    ]
                })
            }
        };
        let client = reqwest::Client::new();
        let response = client.post(&self.endpoint).json(&data).send().await?;
        let payload: Value = response.json().await?;
        let output = serde_yaml::to_string(&payload)?;
        Ok(ToolResult {
            success: true,
            output,
            error: None,
        })
    }
    async fn unpause_task(&self, args: &Value) -> anyhow::Result<ToolResult> {
        let all = args["params"]["all"].as_bool().unwrap_or_default();
        let gid = args["params"]["gid"].as_str().unwrap_or_default();
        let data = match (all, gid.is_empty()) {
            (true, false) => bail!("`all` and `gid` are mutually exclusive"),
            (false, true) => bail!("must provide either `all` or `gid`"),
            (true, true) => json!({
                "jsonrpc": "2.0",
                "id": uuid::Uuid::new_v4().to_string(),
                "method": "aria2.unpauseAll",
                "params": [self.token.clone()],
            }),
            (false, false) => json!({
                    "jsonrpc": "2.0",
                    "id": uuid::Uuid::new_v4().to_string(),
                    "method": "aria2.unpause",
                    "params": [
                    self.token.clone(),
                    gid,
                ]
            }),
        };
        let client = reqwest::Client::new();
        let response = client.post(&self.endpoint).json(&data).send().await?;
        let payload: Value = response.json().await?;
        let output = serde_yaml::to_string(&payload)?;
        Ok(ToolResult {
            success: true,
            output,
            error: None,
        })
    }

    async fn remove_task(&self, args: &Value) -> anyhow::Result<ToolResult> {
        let gid = args["params"]["gid"].as_str().unwrap();
        let force = args["params"]["force"].as_bool().unwrap_or_default();
        let method = match force {
            true => "aria2.forceRemove",
            false => "aria2.remove",
        };
        let data = json!({
            "jsonrpc": "2.0",
            "id": uuid::Uuid::new_v4().to_string(),
            "method": method,
            "params": [
                self.token.clone(),
                gid,
            ]
        });
        let client = reqwest::Client::new();
        let response = client.post(&self.endpoint).json(&data).send().await?;
        let payload: Value = response.json().await?;
        let output = serde_yaml::to_string(&payload)?;
        Ok(ToolResult {
            success: true,
            output,
            error: None,
        })
    }
}

#[async_trait]
impl Tool for Aria2RPCTool {
    fn name(&self) -> &str {
        "aria2rpc_tool"
    }
    fn description(&self) -> &str {
        r##"Control aria2 download manager via JSON-RPC. Supports adding downloads (HTTP/FTP/BitTorrent magnet), removing, pausing/resuming, and querying status/files. Each operation targets downloads by GID (globally unique ID returned by addUri)."##
    }
    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["method", "params"],
            "properties": {
                "method": {
                    "type": "string",
                    "enum": ["addUri", "remove", "pause", "unpause", "tellStatus", "getFiles", "tellDownloads"],
                    "description": r##"The operation to perform:
- addUri: Add a new download (HTTP/FTP/magnet URI)
- remove: Remove a download, optionally force-remove without cleanup
- pause: Pause one or all downloads, optionally force-pause
- unpause: Resume one or all paused downloads
- tellStatus: Query status of a download by GID
- getFiles: Get file list with path/length/completedLength for a download
- tellDownloads: List download tasks"##
                },
                "params": {
                    "type": "object",
                    "description": r##"Method-specific arguments:
- addUri:        { uris: string[] }                                            → returns GID. uris can be HTTP/FTP/magnet URIs or raw BitTorrent info hashes (40-char hex), which will be automatically expanded to magnet URIs at runtime
- remove:        { gid: string, force?: bool }                                 → force skips cleanup
- pause:         { gid?: string, force?: bool, all?: bool }                    → omit gid when all=true
- unpause:       { gid?: string, all?: bool }                                  → omit gid when all=true
- tellStatus:    { gid: string, keys?: string[] }                              → omit keys to get all fields
- getFiles:      { gid: string }
- tellDownloads: {}"##
                }
            }
        })
    }

    async fn execute(&self, args: Value) -> anyhow::Result<ToolResult> {
        debug!("aria2 execute {:?}", args);
        let method = args
            .get("method")
            .ok_or_else(|| anyhow!("missing method"))?
            .as_str()
            .ok_or_else(|| anyhow!("bad method"))?;
        match method {
            "addUri" => self.add_uri(&args).await,
            "remove" => self.remove_task(&args).await,
            "pause" => self.pause_task(&args).await,
            "unpause" => self.unpause_task(&args).await,
            "tellStatus" => self.tell_status(&args).await,
            "getFiles" => self.get_files(&args).await,
            "tellDownloads" => self.tell_downloads(&args).await,
            _ => bail!("unknown method"),
        }
    }
}
