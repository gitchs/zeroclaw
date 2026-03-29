pub mod pg_utils;

use crate::go_utils;
use anyhow::bail;
use pg_utils::{quote_ident, quote_literal};
use serde_derive::Deserialize;
use serde_derive::Serialize;
use serde_json::Value;

pub trait TacnodeGUC {
    fn name(&self) -> &str;
    fn database_level(&self) -> bool;
    fn system_level(&self) -> bool;
    fn session_level(&self) -> bool;
    fn short(&self) -> &'static str;
    fn long(&self) -> &'static str;

    fn generate(&self, dbname: &str) -> anyhow::Result<String>;
}

pub fn tacnode_guc_factory(
    name: &str,
    args: &serde_json::Value,
) -> anyhow::Result<Box<dyn TacnodeGUC>> {
    let guc = match name {
        "experimental_query_history_config" => {
            QueryHistory::from_agent_args(args).map(|g| Box::new(g) as Box<dyn TacnodeGUC>)
        }
        "experimental_show_hidden_catalog" => {
            HiddenCatalog::from_agent_args(args).map(|g| Box::new(g) as Box<dyn TacnodeGUC>)
        }
        "experimental_enable_hidden_grammar" => {
            HiddenGrammar::from_agent_args(args).map(|g| Box::new(g) as Box<dyn TacnodeGUC>)
        }
        "experimental_use_quick_optimizer_mode" => {
            UseQuickOptimizerMode::from_agent_args(args).map(|g| Box::new(g) as Box<dyn TacnodeGUC>)
        }
        _ => bail!("Tacnode GUC type not recognized"),
    }?;
    Ok(guc)
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryHistory {
    pub enabled: bool,
    #[serde(rename = "log_threshold")]
    pub log_threshold: String,
    #[serde(rename = "runtime_stats_enabled")]
    pub runtime_stats_enabled: bool,
    #[serde(rename = "max_placeholder_threshold")]
    pub max_placeholder_threshold: i64,
    #[serde(rename = "max_sql_length_threshold")]
    pub max_sql_length_threshold: i64,
    #[serde(rename = "optimized_logic_plan_fmt_flags")]
    pub optimized_logic_plan_fmt_flags: String,
}

impl QueryHistory {
    pub fn new(
        log_threshold_ms: u64,
        max_placeholder_threshold: Option<i64>,
        max_sql_length_threshold: Option<i64>,
    ) -> Self {
        let log_threshold = go_utils::format_duration(log_threshold_ms);
        let max_placeholder_threshold = max_placeholder_threshold.unwrap_or(10240);
        let max_sql_length_threshold = max_sql_length_threshold.unwrap_or(1024000);
        Self {
            enabled: true,
            log_threshold: log_threshold.into(),
            runtime_stats_enabled: true,
            max_placeholder_threshold,
            max_sql_length_threshold,
            optimized_logic_plan_fmt_flags: "".into(),
        }
    }
    pub fn from_agent_args(args: &serde_json::Value) -> anyhow::Result<Self> {
        let params = args
            .get("params")
            .and_then(Value::as_object)
            .ok_or_else(|| anyhow::anyhow!("no params key"))?;
        let log_threshold = params
            .get("log_threshold")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow::anyhow!("No log threshold provided"))?;
        let runtime_stats_enabled = params.get("runtime_stats_enabled").and_then(Value::as_i64);
        let max_placeholder_threshold = params
            .get("max_placeholder_threshold")
            .and_then(Value::as_i64);
        Ok(Self::new(
            log_threshold,
            runtime_stats_enabled,
            max_placeholder_threshold,
        ))
    }
}

impl TacnodeGUC for QueryHistory {
    fn name(&self) -> &str {
        "experimental_query_history_config"
    }
    fn database_level(&self) -> bool {
        true
    }
    fn system_level(&self) -> bool {
        true
    }

    fn session_level(&self) -> bool {
        false
    }

    fn short(&self) -> &'static str {
        r#"GUC experimental_query_history_config, controls query history collection behavior."#
    }

    fn long(&self) -> &'static str {
        r#"GUC experimental_query_history_config
Controls query history collection behavior. Accepts a JSON object with the following parameters:
- log_threshold — minimum query duration threshold. type bigint, unit is ms. 0 records all queries; queries exceeding this threshold are sampled.
- max_placeholder_threshold — maximum byte size of query placeholders (bind parameters) to store. Default `10240`.
- max_sql_length_threshold — maximum byte size of the SQL text to store. Default `1024000` (~1MB). Longer queries are truncated.
"#
    }

    fn generate(&self, dbname: &str) -> anyhow::Result<String> {
        let settings = serde_json::to_string(self)?;
        if dbname.is_empty() {
            Ok(format!(
                "ALTER SYSTEM SET experimental_query_history_config={};",
                quote_literal(&settings)
            ))
        } else {
            Ok(format!(
                "ALTER DATABASE {} SET experimental_query_history_config={};",
                quote_ident(dbname),
                quote_literal(&settings)
            ))
        }
    }
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HiddenCatalog {}
impl HiddenCatalog {
    pub fn from_agent_args(_args: &Value) -> anyhow::Result<Self> {
        Ok(Self {})
    }
}

impl TacnodeGUC for HiddenCatalog {
    fn name(&self) -> &str {
        "experimental_show_hidden_catalog"
    }
    fn database_level(&self) -> bool {
        false
    }
    fn system_level(&self) -> bool {
        false
    }
    fn session_level(&self) -> bool {
        true
    }
    fn short(&self) -> &'static str {
        "GUC experimental_show_hidden_catalog, Exposes hidden system catalog schemas to unprivileged users."
    }
    fn long(&self) -> &'static str {
        self.short()
    }

    fn generate(&self, _dbname: &str) -> anyhow::Result<String> {
        Ok(format!("SET {}=true;", self.name()))
    }
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HiddenGrammar {}

impl HiddenGrammar {
    pub fn from_agent_args(_args: &Value) -> anyhow::Result<Self> {
        Ok(Self {})
    }
}
impl TacnodeGUC for HiddenGrammar {
    fn name(&self) -> &str {
        "experimental_enable_hidden_grammar"
    }
    fn database_level(&self) -> bool {
        false
    }
    fn system_level(&self) -> bool {
        false
    }
    fn session_level(&self) -> bool {
        true
    }
    fn short(&self) -> &'static str {
        "GUC experimental_enable_hidden_grammar, Activates hidden SQL syntax extensions for experimental use."
    }
    fn long(&self) -> &'static str {
        self.short()
    }

    fn generate(&self, _dbname: &str) -> anyhow::Result<String> {
        Ok(format!("SET {}=true;", self.name()))
    }
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UseQuickOptimizerMode {
    mode: String,
}

impl UseQuickOptimizerMode {
    pub fn new(mode: String) -> Self {
        Self { mode }
    }

    pub fn from_agent_args(args: &Value) -> anyhow::Result<Self> {
        let mode = args
            .get("mode")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("no mode specified"))?;
        Ok(Self::new(mode.to_owned()))
    }
}

impl TacnodeGUC for UseQuickOptimizerMode {
    fn name(&self) -> &str {
        "experimental_use_quick_optimizer_mode"
    }
    fn database_level(&self) -> bool {
        true
    }
    fn system_level(&self) -> bool {
        false
    }
    fn session_level(&self) -> bool {
        false
    }
    fn short(&self) -> &'static str {
        "GUC experimental_use_quick_optimizer_mode: Use quick optimizer mode to reduce planning time at the cost of plan quality"
    }
    fn long(&self) -> &'static str {
        return r##"GUC experimental_use_quick_optimizer_mode: Controls quick optimizer mode for this database, for system metadata queries. Optional mode can be
- 'system' (default): enables quick optimizer, reduces planning time but may produce suboptimal plans.
- 'off': disables quick optimizer, allows broader plan search and avoids full catalog scans
"##;
    }
    fn generate(&self, dbname: &str) -> anyhow::Result<String> {
        if dbname.is_empty() {
            bail!(
                "GUC experimental_use_quick_optimizer_mode only support database_level, dbname is empty"
            );
        }
        let mode = match self.mode.as_str() {
            "off" => "off".to_string(),
            _ => quote_literal(&self.mode),
        };
        Ok(format!(
            "ALTER DATABASE {} SET experimental_use_quick_optimizer_mode={};",
            quote_ident(dbname),
            &mode
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_utils;
    use crate::guc::{QueryHistory, TacnodeGUC};
    use tracing::debug;

    #[test]
    fn test_query_history_config() {
        test_utils::init_tracing();
        let guc = QueryHistory::new(60_000, None, None);
        let line = guc.generate("test").unwrap();
        debug!("guc line = {}", line);
        let expected = r##"ALTER DATABASE "test" SET experimental_query_history_config='{"enabled":true,"log_threshold":"1m","runtime_stats_enabled":true,"max_placeholder_threshold":10240,"max_sql_length_threshold":1024000,"optimized_logic_plan_fmt_flags":""}';"##;
        assert_eq!(line, expected);
    }
}
