pub mod registry;

use crate::config::Config;
use anyhow::Result;

/// Integration status
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum IntegrationStatus {
    /// Fully implemented and ready to use
    Available,
    /// Configured and active
    Active,
    /// Planned but not yet implemented
    ComingSoon,
}

/// Integration category
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
pub enum IntegrationCategory {
    Chat,
    AiModel,
    Productivity,
    MusicAudio,
    SmartHome,
    ToolsAutomation,
    MediaCreative,
    Social,
    Platform,
}

impl IntegrationCategory {
    pub fn label(self) -> &'static str {
        match self {
            Self::Chat => "Chat Providers",
            Self::AiModel => "AI Models",
            Self::Productivity => "Productivity",
            Self::MusicAudio => "Music & Audio",
            Self::SmartHome => "Smart Home",
            Self::ToolsAutomation => "Tools & Automation",
            Self::MediaCreative => "Media & Creative",
            Self::Social => "Social",
            Self::Platform => "Platforms",
        }
    }

    pub fn all() -> &'static [Self] {
        &[
            Self::Chat,
            Self::AiModel,
            Self::Productivity,
            Self::MusicAudio,
            Self::SmartHome,
            Self::ToolsAutomation,
            Self::MediaCreative,
            Self::Social,
            Self::Platform,
        ]
    }
}

/// A registered integration
pub struct IntegrationEntry {
    pub name: &'static str,
    pub description: &'static str,
    pub category: IntegrationCategory,
    pub status_fn: fn(&Config) -> IntegrationStatus,
}

/// Handle the `integrations` CLI command
pub fn handle_command(command: crate::IntegrationCommands, config: &Config) -> Result<()> {
    match command {
        crate::IntegrationCommands::List { category, status } => {
            list_integrations(config, category.as_deref(), status.as_deref())
        }
        crate::IntegrationCommands::Search {
            query,
            category,
            status,
        } => search_integrations(config, &query, category.as_deref(), status.as_deref()),
        crate::IntegrationCommands::Info { name } => show_integration_info(config, &name),
    }
}

fn show_integration_info(config: &Config, name: &str) -> Result<()> {
    let entries = registry::all_integrations();
    let name_lower = name.to_lowercase();

    let Some(entry) = entries.iter().find(|e| e.name.to_lowercase() == name_lower) else {
        anyhow::bail!(
            "Unknown integration: {name}. Check README for supported integrations or run `zeroclaw onboard --interactive` to configure channels/providers."
        );
    };

    let status = (entry.status_fn)(config);
    let (icon, label) = match status {
        IntegrationStatus::Active => ("✅", "Active"),
        IntegrationStatus::Available => ("⚪", "Available"),
        IntegrationStatus::ComingSoon => ("🔜", "Coming Soon"),
    };

    println!();
    println!(
        "  {} {} — {}",
        icon,
        console::style(entry.name).white().bold(),
        entry.description
    );
    println!("  Category: {}", entry.category.label());
    println!("  Status:   {label}");
    println!();

    // Show setup hints based on integration
    match entry.name {
        "Telegram" => {
            println!("  Setup:");
            println!("    1. Message @BotFather on Telegram");
            println!("    2. Create a bot and copy the token");
            println!("    3. Run: zeroclaw onboard --channels-only");
            println!("    4. Start: zeroclaw channel start");
        }
        "Discord" => {
            println!("  Setup:");
            println!("    1. Go to https://discord.com/developers/applications");
            println!("    2. Create app → Bot → Copy token");
            println!("    3. Enable MESSAGE CONTENT intent");
            println!("    4. Run: zeroclaw onboard --channels-only");
        }
        "Slack" => {
            println!("  Setup:");
            println!("    1. Go to https://api.slack.com/apps");
            println!("    2. Create app → Bot Token Scopes → Install");
            println!("    3. Run: zeroclaw onboard --channels-only");
        }
        "OpenRouter" => {
            println!("  Setup:");
            println!("    1. Get API key at https://openrouter.ai/keys");
            println!("    2. Run: zeroclaw onboard");
            println!("    Access 200+ models with one key.");
        }
        "Ollama" => {
            println!("  Setup:");
            println!("    1. Install: brew install ollama");
            println!("    2. Pull a model: ollama pull llama3");
            println!("    3. Set provider to 'ollama' in config.toml");
        }
        "iMessage" => {
            println!("  Setup (macOS only):");
            println!("    Uses AppleScript bridge to send/receive iMessages.");
            println!("    Requires Full Disk Access in System Settings → Privacy.");
        }
        "GitHub" => {
            println!("  Setup:");
            println!("    1. Create a personal access token at https://github.com/settings/tokens");
            println!("    2. Add to config: [integrations.github] token = \"ghp_...\"");
        }
        "Browser" => {
            println!("  Built-in:");
            println!("    ZeroClaw can control Chrome/Chromium for web tasks.");
            println!("    Uses headless browser automation.");
        }
        "Cron" => {
            println!("  Built-in:");
            println!("    Schedule tasks in ~/.zeroclaw/workspace/cron/");
            println!("    Run: zeroclaw cron list");
        }
        "Webhooks" => {
            println!("  Built-in:");
            println!("    HTTP endpoint for external triggers.");
            println!("    Run: zeroclaw gateway");
        }
        _ => {
            if status == IntegrationStatus::ComingSoon {
                println!("  This integration is planned. Stay tuned!");
                println!("  Track progress: https://github.com/theonlyhennygod/zeroclaw");
            }
        }
    }

    println!();
    Ok(())
}

/// Get status icon and label
fn status_icon(status: IntegrationStatus) -> (&'static str, &'static str) {
    match status {
        IntegrationStatus::Active => ("✅", "Active"),
        IntegrationStatus::Available => ("⚪", "Available"),
        IntegrationStatus::ComingSoon => ("🔜", "Coming Soon"),
    }
}

/// Parse category filter from string, supporting aliases
fn parse_category_filter(input: &str) -> Result<IntegrationCategory> {
    let normalized = input.to_lowercase().replace('-', "").replace('_', "");

    match normalized.as_str() {
        "chat" | "chatproviders" | "messaging" => Ok(IntegrationCategory::Chat),
        "ai" | "aimodels" | "aimodel" | "models" | "llm" | "llms" => {
            Ok(IntegrationCategory::AiModel)
        }
        "productivity" | "prod" => Ok(IntegrationCategory::Productivity),
        "music" | "musicaudio" | "audio" => Ok(IntegrationCategory::MusicAudio),
        "smarthome" | "home" | "iot" => Ok(IntegrationCategory::SmartHome),
        "tools" | "toolsautomation" | "automation" => Ok(IntegrationCategory::ToolsAutomation),
        "media" | "mediacreative" | "creative" => Ok(IntegrationCategory::MediaCreative),
        "social" => Ok(IntegrationCategory::Social),
        "platforms" | "platform" => Ok(IntegrationCategory::Platform),
        _ => {
            let valid = [
                "chat",
                "ai",
                "productivity",
                "music",
                "smart-home",
                "tools",
                "media",
                "social",
                "platforms",
            ];
            anyhow::bail!(
                "Unknown category: '{}'. Valid options: {}",
                input,
                valid.join(", ")
            );
        }
    }
}

/// Parse status filter from string
fn parse_status_filter(input: &str) -> Result<IntegrationStatus> {
    let normalized = input.to_lowercase().replace('-', "").replace('_', "");

    match normalized.as_str() {
        "active" | "enabled" | "on" => Ok(IntegrationStatus::Active),
        "available" | "ready" | "off" => Ok(IntegrationStatus::Available),
        "comingsoon" | "soon" | "planned" | "todo" => Ok(IntegrationStatus::ComingSoon),
        _ => {
            anyhow::bail!(
                "Unknown status: '{}'. Valid options: active, available, coming-soon",
                input
            );
        }
    }
}

/// List all integrations grouped by category
fn list_integrations(
    config: &Config,
    category_filter: Option<&str>,
    status_filter: Option<&str>,
) -> Result<()> {
    let entries = registry::all_integrations();

    // Parse filters
    let category_match = category_filter
        .map(|c| parse_category_filter(c))
        .transpose()?;
    let status_match = status_filter.map(|s| parse_status_filter(s)).transpose()?;

    // Group entries by category
    let mut categories: std::collections::BTreeMap<IntegrationCategory, Vec<&IntegrationEntry>> =
        std::collections::BTreeMap::new();

    for entry in &entries {
        // Apply category filter
        if let Some(ref cat) = category_match {
            if entry.category != *cat {
                continue;
            }
        }

        // Apply status filter
        if let Some(ref status) = status_match {
            let entry_status = (entry.status_fn)(config);
            if entry_status != *status {
                continue;
            }
        }

        categories.entry(entry.category).or_default().push(entry);
    }

    println!();
    println!("{}", console::style("ZeroClaw Integrations").white().bold());
    println!();

    if categories.is_empty() {
        println!("  No integrations match the specified filters.");
        println!();
        return Ok(());
    }

    for (category, cat_entries) in categories {
        println!("  {}", console::style(category.label()).cyan().bold());

        for entry in cat_entries {
            let status = (entry.status_fn)(config);
            let (icon, _) = status_icon(status);
            println!(
                "    {} {} — {}",
                icon,
                console::style(entry.name).white(),
                console::style(entry.description).dim()
            );
        }
        println!();
    }

    // Print legend
    println!("  Legend:");
    println!("    ✅ Active (configured)");
    println!("    ⚪ Available");
    println!("    🔜 Coming Soon");
    println!();
    println!("  Run `zeroclaw integrations info <name>` for setup details.");
    println!();

    Ok(())
}

/// Search integrations by query
fn search_integrations(
    config: &Config,
    query: &str,
    category_filter: Option<&str>,
    status_filter: Option<&str>,
) -> Result<()> {
    let entries = registry::all_integrations();
    let query_lower = query.to_lowercase();

    // Parse filters
    let category_match = category_filter
        .map(|c| parse_category_filter(c))
        .transpose()?;
    let status_match = status_filter.map(|s| parse_status_filter(s)).transpose()?;

    let mut results: Vec<&IntegrationEntry> = entries
        .iter()
        .filter(|entry| {
            // Check query match (name or description)
            let matches_query = entry.name.to_lowercase().contains(&query_lower)
                || entry.description.to_lowercase().contains(&query_lower);

            if !matches_query {
                return false;
            }

            // Apply category filter
            if let Some(ref cat) = category_match {
                if entry.category != *cat {
                    return false;
                }
            }

            // Apply status filter
            if let Some(ref status) = status_match {
                let entry_status = (entry.status_fn)(config);
                if entry_status != *status {
                    return false;
                }
            }

            true
        })
        .collect();

    println!();
    println!(
        "{}",
        console::style(format!("Search results for '{}'", query))
            .white()
            .bold()
    );
    println!();

    if results.is_empty() {
        println!("  No integrations found matching your query.");
        println!();
        println!(
            "  Try a different search term or run `zeroclaw integrations list` to see all options."
        );
        println!();
        return Ok(());
    }

    // Sort by name for consistent output
    results.sort_by_key(|e| e.name);

    for entry in results {
        let status = (entry.status_fn)(config);
        let (icon, _) = status_icon(status);
        println!(
            "  {} {} — {} [{}]",
            icon,
            console::style(entry.name).white(),
            console::style(entry.description).dim(),
            console::style(entry.category.label()).cyan()
        );
    }

    println!();
    println!("  Run `zeroclaw integrations info <name>` for setup details.");
    println!();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integration_category_all_includes_every_variant_once() {
        let all = IntegrationCategory::all();
        assert_eq!(all.len(), 9);

        let labels: Vec<&str> = all.iter().map(|cat| cat.label()).collect();
        assert!(labels.contains(&"Chat Providers"));
        assert!(labels.contains(&"AI Models"));
        assert!(labels.contains(&"Productivity"));
        assert!(labels.contains(&"Music & Audio"));
        assert!(labels.contains(&"Smart Home"));
        assert!(labels.contains(&"Tools & Automation"));
        assert!(labels.contains(&"Media & Creative"));
        assert!(labels.contains(&"Social"));
        assert!(labels.contains(&"Platforms"));
    }

    #[test]
    fn handle_command_info_is_case_insensitive_for_known_integrations() {
        let config = Config::default();
        let first_name = registry::all_integrations()
            .first()
            .expect("registry should define at least one integration")
            .name
            .to_lowercase();

        let result = handle_command(
            crate::IntegrationCommands::Info { name: first_name },
            &config,
        );

        assert!(result.is_ok());
    }

    #[test]
    fn handle_command_info_returns_error_for_unknown_integration() {
        let config = Config::default();
        let result = handle_command(
            crate::IntegrationCommands::Info {
                name: "definitely-not-a-real-integration".into(),
            },
            &config,
        );

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Unknown integration"));
    }

    #[test]
    fn handle_command_list_returns_ok() {
        let config = Config::default();
        let result = handle_command(
            crate::IntegrationCommands::List {
                category: None,
                status: None,
            },
            &config,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn handle_command_list_with_category_filter() {
        let config = Config::default();
        let result = handle_command(
            crate::IntegrationCommands::List {
                category: Some("chat".into()),
                status: None,
            },
            &config,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn handle_command_list_with_invalid_category() {
        let config = Config::default();
        let result = handle_command(
            crate::IntegrationCommands::List {
                category: Some("invalid-category".into()),
                status: None,
            },
            &config,
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Unknown category"));
    }

    #[test]
    fn handle_command_list_with_status_filter() {
        let config = Config::default();
        let result = handle_command(
            crate::IntegrationCommands::List {
                category: None,
                status: Some("available".into()),
            },
            &config,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn handle_command_list_with_invalid_status() {
        let config = Config::default();
        let result = handle_command(
            crate::IntegrationCommands::List {
                category: None,
                status: Some("invalid-status".into()),
            },
            &config,
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Unknown status"));
    }

    #[test]
    fn handle_command_search_returns_ok() {
        let config = Config::default();
        let result = handle_command(
            crate::IntegrationCommands::Search {
                query: "telegram".into(),
                category: None,
                status: None,
            },
            &config,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn handle_command_search_no_results_is_ok() {
        let config = Config::default();
        let result = handle_command(
            crate::IntegrationCommands::Search {
                query: "xyznonexistent123".into(),
                category: None,
                status: None,
            },
            &config,
        );
        assert!(result.is_ok()); // No results is not an error
    }

    #[test]
    fn parse_category_filter_handles_aliases() {
        assert!(matches!(
            parse_category_filter("ai"),
            Ok(IntegrationCategory::AiModel)
        ));
        assert!(matches!(
            parse_category_filter("AI"),
            Ok(IntegrationCategory::AiModel)
        ));
        assert!(matches!(
            parse_category_filter("models"),
            Ok(IntegrationCategory::AiModel)
        ));
        assert!(matches!(
            parse_category_filter("smart-home"),
            Ok(IntegrationCategory::SmartHome)
        ));
        assert!(matches!(
            parse_category_filter("smarthome"),
            Ok(IntegrationCategory::SmartHome)
        ));
    }

    #[test]
    fn parse_status_filter_handles_aliases() {
        assert!(matches!(
            parse_status_filter("active"),
            Ok(IntegrationStatus::Active)
        ));
        assert!(matches!(
            parse_status_filter("enabled"),
            Ok(IntegrationStatus::Active)
        ));
        assert!(matches!(
            parse_status_filter("available"),
            Ok(IntegrationStatus::Available)
        ));
        assert!(matches!(
            parse_status_filter("coming-soon"),
            Ok(IntegrationStatus::ComingSoon)
        ));
        assert!(matches!(
            parse_status_filter("soon"),
            Ok(IntegrationStatus::ComingSoon)
        ));
    }
}
