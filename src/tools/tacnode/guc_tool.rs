use anyhow::bail;
use async_trait::async_trait;
use serde_json::Value;
use super::super::{{Tool, ToolResult}};
use serde_json::{json};
use tacnode::guc::{HiddenCatalog, HiddenGrammar, QueryHistory, TacnodeGUC, UseQuickOptimizerMode};
use tracing::debug;

pub struct GUCTool {}


impl GUCTool {
    pub fn new() -> Self {
        Self {}
    }
    fn list_guc(&self, _args: Value) -> anyhow::Result<ToolResult> {
        let guc_list = vec![
            QueryHistory::default().short(),
            HiddenCatalog::default().short(),
            HiddenGrammar::default().short(),
            UseQuickOptimizerMode::default().short(),
        ];
        let wrapper = json!({
            "guc": guc_list,
        });
        let output = serde_yaml::to_string(&wrapper)?;
        debug!("guc list = {}", &output);
        Ok(ToolResult{
            success: true,
            output,
            error: None,
        })
    }

    fn generate(&self, args: Value) -> anyhow::Result<ToolResult> {
        let name = args.get("name").unwrap().as_str().unwrap();
       let guc = tacnode::guc::tacnode_guc_factory(name, &args)?;
        let dbname = args.get("dbname").unwrap_or_default().as_str().unwrap_or_default();
        let output = guc.generate(dbname)?;
        Ok(ToolResult{
            success: true,
            output,
            error: None,
        })
    }


    fn describe_guc(&self, args: Value) -> anyhow::Result<ToolResult> {
        let name = args.get("name").unwrap().as_str().unwrap();
        let description = match name {
            "experimental_query_history_config" => QueryHistory::default().long(),
            "experimental_show_hidden_catalog" => HiddenCatalog::default().long(),
            "experimental_enable_hidden_grammar" => HiddenGrammar::default().long(),
            "experimental_use_quick_optimizer_mode" => UseQuickOptimizerMode::default().long(),
            _ => anyhow::bail!("unknown GUC {}, call action list-guc first", name)
        };
        Ok(ToolResult{
            success: true,
            output: description.to_string(),
            error: None,
        })
    }

}

#[async_trait]
impl Tool for GUCTool {
    fn name(&self) -> &'static str {
        "tacnode_guc_tool"
    }
    fn description(&self) -> &'static str {
        r##"Manage GUC settings for tacnode engine.
Workflow for 'generate':
  1. Call 'describe-guc' with target 'name' to get its parameter schema
  2. Call 'generate' with 'name', optional 'dbname', and a 'params' object matching the schema from step 1

Actions:
- 'list-guc': list all available GUC names
- 'describe-guc': get parameter schema for a specific GUC (required before generate)
- 'generate': produce SET statements. Requires 'name' and 'params' object
"##
    }
    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["list-guc", "describe-guc", "generate"],
                    "description": "Action to perform. For 'generate', call 'describe-guc' first to get the parameter schema of the target GUC, then pass the required parameters accordingly."
                },
                "name": {
                    "type": "string",
                    "description": "guc name, required for describe-guc and generate"
                },
                "dbname": {
                    "type": "string",
                    "description": "dbname, optional for generate"
                },
                "params": {
                    "type": "object",
                    "description": "GUC-specific parameters. Call describe-guc first to get the required fields for the target GUC name."
                }
            },
            "required": ["action"]
        })
    }

    async fn execute(&self, args: Value) -> anyhow::Result<ToolResult> {
        debug!("tacnode::guc::execute: {}", args);
        let action = args.get("action").unwrap().as_str().unwrap();
        let retval = match action.as_ref() {
            "list-guc" => self.list_guc(args),
            "describe-guc" => self.describe_guc(args),
            "generate" => self.generate(args),
            _ => bail!(format!("unknown action {}", action))
        };
        debug!("tacnode::guc::execute: {:#?}", retval);
        retval
    }
}