//! Timezone-aware date/time dimension key derivation and row seeding.

use chrono::{DateTime, Datelike, NaiveDate, Timelike, Utc};
use chrono_tz::Tz;
use rusqlite::{Connection, params};

use crate::error::{Error, Result};

/// Derived dimension keys for one UTC timestamp in a local timezone.
#[derive(Debug, Clone, PartialEq)]
pub struct LocalKeys {
    pub date_key: String,
    pub time_key: String,
}

/// Read the configured IANA timezone (config key `timezone`).
pub fn configured_timezone(conn: &Connection) -> Result<Tz> {
    let name: String = conn
        .query_row(
            "SELECT value FROM config WHERE key = 'timezone'",
            [],
            |row| row.get(0),
        )
        .unwrap_or_else(|_| "America/Los_Angeles".to_string());
    name.parse::<Tz>()
        .map_err(|_| Error::Config(format!("invalid timezone '{name}'")))
}

/// Read the configured core-hours window (start inclusive, end exclusive).
pub fn configured_core_hours(conn: &Connection) -> (u32, u32) {
    let read = |key: &str, default: u32| -> u32 {
        conn.query_row("SELECT value FROM config WHERE key = ?1", [key], |row| {
            row.get::<_, String>(0)
        })
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
    };
    (read("core_hours_start", 9), read("core_hours_end", 17))
}

/// Parse an ISO-8601 UTC timestamp.
pub fn parse_utc(timestamp: &str) -> Result<DateTime<Utc>> {
    timestamp
        .parse::<DateTime<Utc>>()
        .map_err(|error| Error::InvalidArgument(format!("bad timestamp '{timestamp}': {error}")))
}

/// Derive local date/time keys for a UTC timestamp.
pub fn derive_local_keys(timestamp_utc: &DateTime<Utc>, timezone: Tz) -> LocalKeys {
    let local = timestamp_utc.with_timezone(&timezone);
    LocalKeys {
        date_key: local.format("%Y-%m-%d").to_string(),
        time_key: format!("{:02}:00", local.hour()),
    }
}

fn day_of_half(date: NaiveDate) -> i64 {
    let half_start_month = if date.month() <= 6 { 1 } else { 7 };
    let half_start = NaiveDate::from_ymd_opt(date.year(), half_start_month, 1).unwrap();
    (date - half_start).num_days() + 1
}

fn day_of_quarter(date: NaiveDate) -> i64 {
    let quarter_start_month = ((date.month() - 1) / 3) * 3 + 1;
    let quarter_start = NaiveDate::from_ymd_opt(date.year(), quarter_start_month, 1).unwrap();
    (date - quarter_start).num_days() + 1
}

/// Ensure a `dim_date` row exists for a `YYYY-MM-DD` key.
pub fn ensure_date_row(conn: &Connection, date_key: &str) -> Result<()> {
    let date = NaiveDate::parse_from_str(date_key, "%Y-%m-%d")
        .map_err(|error| Error::InvalidArgument(format!("bad date key '{date_key}': {error}")))?;
    let year = date.year();
    let quarter = ((date.month() - 1) / 3 + 1) as i64;
    let half = if date.month() <= 6 { 1 } else { 2 };
    let iso_week = date.iso_week();
    let day_of_week = date.weekday().number_from_monday() as i64;
    let quarter_start_month = ((date.month() - 1) / 3) * 3 + 1;
    conn.execute(
        "INSERT OR IGNORE INTO dim_date (
            date_key, year, quarter, month, day_of_month, day_of_week, is_weekend,
            week_of_year, week_key, month_key, quarter_key, year_key, half_key,
            day_of_quarter, day_of_year, day_of_half, week_of_quarter,
            month_of_quarter, month_of_half
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19)",
        params![
            date_key,
            year,
            quarter,
            date.month() as i64,
            date.day() as i64,
            day_of_week,
            (day_of_week >= 6) as i64,
            iso_week.week() as i64,
            format!("{}-W{:02}", iso_week.year(), iso_week.week()),
            format!("{year}-{:02}", date.month()),
            format!("{year}-Q{quarter}"),
            format!("{year}"),
            format!("{year}-H{half}"),
            day_of_quarter(date),
            date.ordinal() as i64,
            day_of_half(date),
            (day_of_quarter(date) - 1) / 7 + 1,
            (date.month() as i64 - quarter_start_month as i64) + 1,
            if half == 1 { date.month() as i64 } else { date.month() as i64 - 6 },
        ],
    )?;
    Ok(())
}

/// Ensure a `dim_time` row exists for an `HH:00` key.
pub fn ensure_time_row(conn: &Connection, time_key: &str, core_hours: (u32, u32)) -> Result<()> {
    let hour: u32 = time_key
        .split(':')
        .next()
        .and_then(|text| text.parse().ok())
        .ok_or_else(|| Error::InvalidArgument(format!("bad time key '{time_key}'")))?;
    let hour_12 = match hour % 12 {
        0 => 12,
        other => other,
    };
    let am_pm = if hour < 12 { "AM" } else { "PM" };
    let bucket = match hour {
        0..=5 => "Night",
        6..=11 => "Morning",
        12..=17 => "Afternoon",
        _ => "Evening",
    };
    let is_core = hour >= core_hours.0 && hour < core_hours.1;
    conn.execute(
        "INSERT OR IGNORE INTO dim_time (time_key, hour, hour_12, am_pm, time_bucket, is_core_hours)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![time_key, hour as i64, hour_12 as i64, am_pm, bucket, is_core as i64],
    )?;
    Ok(())
}

/// Derive keys for a timestamp and make sure both dimension rows exist.
pub fn ensure_keys_for_timestamp(
    conn: &Connection,
    timestamp: &str,
    timezone: Tz,
    core_hours: (u32, u32),
) -> Result<LocalKeys> {
    let utc = parse_utc(timestamp)?;
    let keys = derive_local_keys(&utc, timezone);
    ensure_date_row(conn, &keys.date_key)?;
    ensure_time_row(conn, &keys.time_key, core_hours)?;
    Ok(keys)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use chrono_tz::America::Los_Angeles;

    #[test]
    fn derives_local_keys_across_dst() {
        // Winter (PST, UTC-8): 10:00Z -> 02:00 local.
        let winter = Utc.with_ymd_and_hms(2026, 1, 15, 10, 0, 0).unwrap();
        let keys = derive_local_keys(&winter, Los_Angeles);
        assert_eq!(keys.date_key, "2026-01-15");
        assert_eq!(keys.time_key, "02:00");

        // Summer (PDT, UTC-7): 10:00Z -> 03:00 local.
        let summer = Utc.with_ymd_and_hms(2026, 7, 15, 10, 0, 0).unwrap();
        let keys = derive_local_keys(&summer, Los_Angeles);
        assert_eq!(keys.time_key, "03:00");
    }

    #[test]
    fn date_rollover_at_local_midnight() {
        // 06:00Z on Jan 2 is 22:00 on Jan 1 in Los Angeles.
        let timestamp = Utc.with_ymd_and_hms(2026, 1, 2, 6, 0, 0).unwrap();
        let keys = derive_local_keys(&timestamp, Los_Angeles);
        assert_eq!(keys.date_key, "2026-01-01");
        assert_eq!(keys.time_key, "22:00");
    }
}
