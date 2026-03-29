use tracing::debug;

pub fn format_duration(ms: u64) -> String {
    if ms == 0 {
        return "0ms".to_string();
    }
    let hours = ms / 3600_000;
    let ms = ms % 3600_000;
    let minutes = ms / 60_000;
    let ms = ms % 60_000;
    let seconds = ms / 1000;
    let ms = ms % 1000;
    let mut parts = vec![];
    if hours > 0 {
        parts.push(format!("{}h", hours));
    }
    if minutes > 0 {
        parts.push(format!("{}m", minutes));
    }
    if seconds > 0 {
        parts.push(format!("{}s", seconds));
    }
    if ms > 0 {
        parts.push(format!("{}ms", ms));
    }
    debug!(
        "hour = {}, minute = {}, second = {}, ms = {}",
        hours, minutes, seconds, ms
    );
    parts.join("")
}

#[cfg(test)]
mod tests {
    use crate::go_utils::format_duration;
    use crate::test_utils;

    #[test]
    fn test_format_duration_0ms() {
        let duration = format_duration(0);
        assert_eq!(duration, "0ms".to_string());
    }
    #[test]
    fn test_format_duration_10ms() {
        let duration = format_duration(10);
        assert_eq!(duration, "10ms".to_string());
    }

    #[test]
    fn test_format_duration_1h10m5s30ms() {
        test_utils::init_tracing();
        let duration = format_duration(4205030);
        assert_eq!(duration, "1h10m5s30ms".to_string());
    }
}
