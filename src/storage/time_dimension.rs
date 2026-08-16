//! Timezone-aware date/time dimension key derivation and row seeding.

use chrono::{DateTime, Datelike, Duration, NaiveDate, Timelike, Utc};
use chrono_tz::Tz;
use rusqlite::{Connection, params};

use crate::error::{Error, Result};

/// Fallback IANA zone when the warehouse has no usable `timezone` config.
pub const DEFAULT_TIMEZONE: &str = "America/Los_Angeles";

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
        .unwrap_or_else(|_| DEFAULT_TIMEZONE.to_string());
    name.parse::<Tz>()
        .map_err(|_| Error::Config(format!("invalid timezone '{name}'")))
}

/// The calendar date `instant` falls on in `timezone`. DST-aware, because the
/// conversion goes through the zone's full transition table rather than a
/// fixed offset.
///
/// This is the single rule that relates a point in time to a `date_key`: the
/// same rule [`derive_local_keys`] applies to stored facts. Any caller that
/// needs a "today" to compare against `date_key` values must go through here
/// (or [`today`]) rather than taking a UTC calendar date, otherwise the two
/// calendars disagree for the hours when the zone's local date and the UTC
/// date differ — up to 14 hours a day, every day.
pub fn local_date_for(instant: DateTime<Utc>, timezone: Tz) -> NaiveDate {
    instant.with_timezone(&timezone).date_naive()
}

/// "Today" in the warehouse's configured timezone, at `instant`.
///
/// Takes the instant explicitly so callers (and tests) can be deterministic.
pub fn today_as_of(conn: &Connection, instant: DateTime<Utc>) -> Result<NaiveDate> {
    Ok(local_date_for(instant, configured_timezone(conn)?))
}

/// "Today" in the warehouse's configured timezone, read from the clock.
pub fn today(conn: &Connection) -> Result<NaiveDate> {
    today_as_of(conn, Utc::now())
}

/// "Today" in the warehouse's configured timezone, falling back to
/// [`DEFAULT_TIMEZONE`] when the configured value cannot be parsed.
///
/// For infallible constructors. A zone this rejects would also have failed
/// every key derivation during sync, so the fallback is unreachable in a
/// warehouse that holds any data.
pub fn today_or_default(conn: &Connection) -> NaiveDate {
    let timezone = configured_timezone(conn).unwrap_or_else(|_| {
        DEFAULT_TIMEZONE
            .parse::<Tz>()
            .expect("DEFAULT_TIMEZONE is a valid IANA zone")
    });
    local_date_for(Utc::now(), timezone)
}

/// The last *fully elapsed* day in the warehouse's configured timezone, at
/// `instant`.
///
/// Coverage records must never claim a day that is still in progress: items
/// created later in that same local day would be permanently skipped by any
/// reader that trusts the claim. Sealing only completed days makes the
/// watermark lag the truth instead of overstating it.
pub fn last_complete_day_as_of(conn: &Connection, instant: DateTime<Utc>) -> Result<NaiveDate> {
    Ok(today_as_of(conn, instant)? - Duration::days(1))
}

/// The last fully elapsed day in the configured timezone, read from the clock.
pub fn last_complete_day(conn: &Connection) -> Result<NaiveDate> {
    last_complete_day_as_of(conn, Utc::now())
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

    fn set_timezone(conn: &Connection, name: &str) {
        conn.execute(
            "INSERT INTO config (key, value) VALUES ('timezone', ?1)
             ON CONFLICT (key) DO UPDATE SET value = excluded.value",
            [name],
        )
        .unwrap();
    }

    /// `today` must agree with the calendar the stored `date_key` values use,
    /// which is the configured zone's calendar — not the UTC one. Evening in a
    /// zone behind UTC is already tomorrow in UTC.
    #[test]
    fn today_uses_the_configured_zone_not_utc() {
        let warehouse = crate::GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();
        set_timezone(conn, "America/Los_Angeles");

        // 2026-07-01T01:00:00Z is 2026-06-30 18:00 PDT: still June locally.
        let instant = Utc.with_ymd_and_hms(2026, 7, 1, 1, 0, 0).unwrap();
        assert_eq!(
            today_as_of(conn, instant).unwrap(),
            NaiveDate::from_ymd_opt(2026, 6, 30).unwrap()
        );
        assert_ne!(today_as_of(conn, instant).unwrap(), instant.date_naive());

        // Same rule under standard time (UTC-8), so the fix is not an offset
        // baked in at one time of year.
        let winter = Utc.with_ymd_and_hms(2026, 1, 1, 1, 0, 0).unwrap();
        assert_eq!(
            today_as_of(conn, winter).unwrap(),
            NaiveDate::from_ymd_opt(2025, 12, 31).unwrap()
        );
    }

    /// The zone is read from config, so two warehouses configured differently
    /// resolve different calendars at the same instant. Zones 26 hours apart
    /// can never share a local date.
    #[test]
    fn today_is_config_driven_in_both_directions() {
        let east = crate::GithubDW::open_in_memory().unwrap();
        set_timezone(east.connection(), "Pacific/Kiritimati"); // UTC+14
        let west = crate::GithubDW::open_in_memory().unwrap();
        set_timezone(west.connection(), "Etc/GMT+12"); // UTC-12

        let instant = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let east_today = today_as_of(east.connection(), instant).unwrap();
        let west_today = today_as_of(west.connection(), instant).unwrap();
        assert_eq!(east_today, NaiveDate::from_ymd_opt(2026, 3, 11).unwrap());
        assert_eq!(west_today, NaiveDate::from_ymd_opt(2026, 3, 10).unwrap());
        assert_ne!(east_today, west_today, "the zone comes from config");
    }

    /// Coverage must be sealed only through the last completed local day.
    #[test]
    fn last_complete_day_trails_local_today() {
        let warehouse = crate::GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();
        set_timezone(conn, "America/Los_Angeles");

        // Mid-afternoon UTC on 2026-08-16 is still morning of the 16th in LA:
        // the 16th is in progress, so only the 15th is complete.
        let instant = Utc.with_ymd_and_hms(2026, 8, 16, 16, 31, 57).unwrap();
        assert_eq!(
            last_complete_day_as_of(conn, instant).unwrap(),
            NaiveDate::from_ymd_opt(2026, 8, 15).unwrap()
        );

        // Evening in LA: UTC has rolled to the 17th, but the 17th has not
        // started locally, so the last complete local day is still the 15th.
        let evening = Utc.with_ymd_and_hms(2026, 8, 17, 3, 0, 0).unwrap();
        assert_eq!(
            last_complete_day_as_of(conn, evening).unwrap(),
            NaiveDate::from_ymd_opt(2026, 8, 15).unwrap()
        );
    }

    /// `dim_date.week_key` must use the ISO week-*year*, which is what makes
    /// the dimension self-consistent with the fact keys it is derived from at
    /// a year boundary: 2024-12-30 belongs to ISO week 2025-W01.
    #[test]
    fn week_key_uses_the_iso_week_year() {
        let warehouse = crate::GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();
        ensure_date_row(conn, "2024-12-30").unwrap();
        let (week_key, year_key): (String, String) = conn
            .query_row(
                "SELECT week_key, year_key FROM dim_date WHERE date_key = '2024-12-30'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(week_key, "2025-W01");
        assert_eq!(year_key, "2024", "calendar year is unaffected");
    }
}
