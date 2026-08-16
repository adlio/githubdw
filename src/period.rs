//! The `Period` type: every time filter and metric window.

use chrono::{DateTime, Datelike, Duration, NaiveDate, Utc};
use chrono_tz::Tz;

use crate::error::{Error, Result};

/// A named time window with period-over-period semantics.
#[derive(Debug, Clone, PartialEq)]
pub enum Period {
    Year(i32),
    Half(i32, u8),
    Quarter(i32, u8),
    Month(i32, u8),
    Week(i32, u8),
    /// Last N days ending on a date.
    Rolling(u32, NaiveDate),
}

impl Period {
    /// Canonical key string: `2026`, `2026-H1`, `2026-Q1`, `2026-01`,
    /// `2026-W02`, `last-30`.
    pub fn to_key(&self) -> String {
        match self {
            Period::Year(year) => format!("{year}"),
            Period::Half(year, half) => format!("{year}-H{half}"),
            Period::Quarter(year, quarter) => format!("{year}-Q{quarter}"),
            Period::Month(year, month) => format!("{year}-{month:02}"),
            Period::Week(year, week) => format!("{year}-W{week:02}"),
            Period::Rolling(days, _) => format!("last-{days}"),
        }
    }

    /// Parse `2026`, `2026-H1`, `2026-Q1`, `2026-01`, `2026-M1`, `2026-W02`,
    /// `last-N`, `this-*`, `previous-*` against the UTC calendar date.
    ///
    /// # Deprecated
    ///
    /// Relative forms (`this-*`, `previous-*`, `last-N`) are resolved against
    /// a reference date, and that date must come from the same calendar the
    /// warehouse's `date_key` columns use — the configured IANA zone, not UTC.
    /// Using the UTC date shifts every relative window by one day for the
    /// hours when the two calendars disagree, which at a quarter or year
    /// boundary answers about the wrong period entirely.
    ///
    /// Use [`Period::parse_in_zone`] (or [`Period::parse_with_reference`] with
    /// `storage::time_dimension::today`) instead.
    #[deprecated(
        since = "0.2.2",
        note = "resolves relative periods against the UTC date; use parse_in_zone or \
                parse_with_reference with the warehouse timezone"
    )]
    pub fn parse(text: &str) -> Result<Self> {
        Self::parse_with_reference(text, Utc::now().date_naive())
    }

    /// Parse against "now" as seen in `timezone`. DST-aware.
    pub fn parse_in_zone(text: &str, timezone: Tz) -> Result<Self> {
        Self::parse_as_of(text, Utc::now(), timezone)
    }

    /// Parse against a specific instant as seen in `timezone`.
    ///
    /// The injectable form: pass the instant rather than reading a clock so
    /// relative-period resolution is deterministic.
    pub fn parse_as_of(text: &str, instant: DateTime<Utc>, timezone: Tz) -> Result<Self> {
        Self::parse_with_reference(text, instant.with_timezone(&timezone).date_naive())
    }

    /// Parse with an explicit reference date (testable).
    pub fn parse_with_reference(text: &str, reference: NaiveDate) -> Result<Self> {
        let text = text.trim();
        let lowered = text.to_lowercase();
        match lowered.as_str() {
            "this-week" => {
                let week = reference.iso_week();
                return Ok(Period::Week(week.year(), week.week() as u8));
            }
            "this-month" => {
                return Ok(Period::Month(reference.year(), reference.month() as u8));
            }
            "this-quarter" => {
                return Ok(Period::Quarter(
                    reference.year(),
                    ((reference.month() - 1) / 3 + 1) as u8,
                ));
            }
            "this-year" => return Ok(Period::Year(reference.year())),
            "previous-week" => {
                let week = reference.iso_week();
                return Ok(Period::Week(week.year(), week.week() as u8).previous());
            }
            "previous-month" => {
                return Ok(Period::Month(reference.year(), reference.month() as u8).previous());
            }
            "previous-quarter" => {
                return Ok(Period::Quarter(
                    reference.year(),
                    ((reference.month() - 1) / 3 + 1) as u8,
                )
                .previous());
            }
            "previous-year" => return Ok(Period::Year(reference.year()).previous()),
            _ => {}
        }
        if let Some(days_text) = lowered.strip_prefix("last-") {
            let days: u32 = days_text
                .parse()
                .map_err(|_| Error::InvalidArgument(format!("bad rolling period '{text}'")))?;
            return Ok(Period::Rolling(days, reference));
        }
        if let Some((year_text, rest)) = text.split_once('-') {
            let year: i32 = year_text
                .parse()
                .map_err(|_| Error::InvalidArgument(format!("bad period '{text}'")))?;
            let rest_upper = rest.to_uppercase();
            if let Some(half) = rest_upper.strip_prefix('H') {
                let half: u8 = half
                    .parse()
                    .map_err(|_| Error::InvalidArgument(format!("bad half '{text}'")))?;
                if !(1..=2).contains(&half) {
                    return Err(Error::InvalidArgument(format!("bad half '{text}'")));
                }
                return Ok(Period::Half(year, half));
            }
            if let Some(quarter) = rest_upper.strip_prefix('Q') {
                let quarter: u8 = quarter
                    .parse()
                    .map_err(|_| Error::InvalidArgument(format!("bad quarter '{text}'")))?;
                if !(1..=4).contains(&quarter) {
                    return Err(Error::InvalidArgument(format!("bad quarter '{text}'")));
                }
                return Ok(Period::Quarter(year, quarter));
            }
            if let Some(week) = rest_upper.strip_prefix('W') {
                let week: u8 = week
                    .parse()
                    .map_err(|_| Error::InvalidArgument(format!("bad week '{text}'")))?;
                return Ok(Period::Week(year, week));
            }
            if let Some(month) = rest_upper.strip_prefix('M') {
                let month: u8 = month
                    .parse()
                    .map_err(|_| Error::InvalidArgument(format!("bad month '{text}'")))?;
                return Ok(Period::Month(year, month));
            }
            let month: u8 = rest
                .parse()
                .map_err(|_| Error::InvalidArgument(format!("bad period '{text}'")))?;
            if !(1..=12).contains(&month) {
                return Err(Error::InvalidArgument(format!("bad month '{text}'")));
            }
            return Ok(Period::Month(year, month));
        }
        let year: i32 = text
            .parse()
            .map_err(|_| Error::InvalidArgument(format!("bad period '{text}'")))?;
        Ok(Period::Year(year))
    }

    /// Inclusive `(start, end)` dates of the period.
    pub fn date_range(&self) -> (NaiveDate, NaiveDate) {
        match self {
            Period::Year(year) => (
                NaiveDate::from_ymd_opt(*year, 1, 1).unwrap(),
                NaiveDate::from_ymd_opt(*year, 12, 31).unwrap(),
            ),
            Period::Half(year, half) => {
                let (start_month, end_month) = if *half == 1 { (1, 6) } else { (7, 12) };
                (
                    NaiveDate::from_ymd_opt(*year, start_month, 1).unwrap(),
                    last_day_of_month(*year, end_month),
                )
            }
            Period::Quarter(year, quarter) => {
                let start_month = (*quarter as u32 - 1) * 3 + 1;
                (
                    NaiveDate::from_ymd_opt(*year, start_month, 1).unwrap(),
                    last_day_of_month(*year, start_month + 2),
                )
            }
            Period::Month(year, month) => (
                NaiveDate::from_ymd_opt(*year, *month as u32, 1).unwrap(),
                last_day_of_month(*year, *month as u32),
            ),
            Period::Week(year, week) => {
                let start = NaiveDate::from_isoywd_opt(*year, *week as u32, chrono::Weekday::Mon)
                    .unwrap_or_else(|| NaiveDate::from_ymd_opt(*year, 1, 1).unwrap());
                (start, start + Duration::days(6))
            }
            Period::Rolling(days, end) => (*end - Duration::days(*days as i64 - 1), *end),
        }
    }

    /// The immediately preceding period of the same granularity.
    pub fn previous(&self) -> Period {
        match self {
            Period::Year(year) => Period::Year(year - 1),
            Period::Half(year, 1) => Period::Half(year - 1, 2),
            Period::Half(year, _) => Period::Half(*year, 1),
            Period::Quarter(year, 1) => Period::Quarter(year - 1, 4),
            Period::Quarter(year, quarter) => Period::Quarter(*year, quarter - 1),
            Period::Month(year, 1) => Period::Month(year - 1, 12),
            Period::Month(year, month) => Period::Month(*year, month - 1),
            Period::Week(year, week) => {
                if *week <= 1 {
                    let last_week = NaiveDate::from_ymd_opt(year - 1, 12, 28)
                        .unwrap()
                        .iso_week()
                        .week() as u8;
                    Period::Week(year - 1, last_week)
                } else {
                    Period::Week(*year, week - 1)
                }
            }
            Period::Rolling(days, end) => {
                Period::Rolling(*days, *end - Duration::days(*days as i64))
            }
        }
    }

    /// Which `dim_date` column equality-filters this period.
    pub fn date_column(&self) -> &'static str {
        match self {
            Period::Year(..) => "year_key",
            Period::Half(..) => "half_key",
            Period::Quarter(..) => "quarter_key",
            Period::Month(..) => "month_key",
            Period::Week(..) => "week_key",
            Period::Rolling(..) => "date_key",
        }
    }

    /// The positional column for apples-to-apples truncation of the
    /// previous period. None for Rolling (always complete).
    pub fn position_column(&self) -> Option<&'static str> {
        match self {
            Period::Year(..) => Some("day_of_year"),
            Period::Half(..) => Some("day_of_half"),
            Period::Quarter(..) => Some("day_of_quarter"),
            Period::Month(..) => Some("day_of_month"),
            Period::Week(..) => Some("day_of_week"),
            Period::Rolling(..) => None,
        }
    }

    /// Days elapsed within the period as of a reference date.
    pub fn days_elapsed(&self, reference: NaiveDate) -> i64 {
        let (start, end) = self.date_range();
        let effective = reference.min(end);
        ((effective - start).num_days() + 1).max(0)
    }

    /// Is the period still in progress at the reference date?
    pub fn is_partial(&self, reference: NaiveDate) -> bool {
        match self {
            Period::Rolling(..) => false,
            _ => {
                let (start, end) = self.date_range();
                reference >= start && reference < end
            }
        }
    }

    /// The period's end capped at the reference date.
    pub fn effective_end_date(&self, reference: NaiveDate) -> NaiveDate {
        let (_, end) = self.date_range();
        end.min(reference)
    }

    /// Pick the dim_date column for an arbitrary period key string.
    pub fn column_for_key(key: &str) -> &'static str {
        if key.contains("-Q") {
            "quarter_key"
        } else if key.contains("-W") {
            "week_key"
        } else if key.contains("-H") {
            "half_key"
        } else if key.contains('-') {
            "month_key"
        } else {
            "year_key"
        }
    }
}

fn last_day_of_month(year: i32, month: u32) -> NaiveDate {
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    NaiveDate::from_ymd_opt(next_year, next_month, 1).unwrap() - Duration::days(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use chrono_tz::America::Los_Angeles;
    use chrono_tz::Pacific::Kiritimati;

    /// Reference date for the absolute forms, where it is not consulted.
    fn any_reference() -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 7, 24).unwrap()
    }

    fn parse(text: &str) -> Result<Period> {
        Period::parse_with_reference(text, any_reference())
    }

    #[test]
    fn parses_all_forms() {
        assert_eq!(parse("2026").unwrap(), Period::Year(2026));
        assert_eq!(parse("2026-H1").unwrap(), Period::Half(2026, 1));
        assert_eq!(parse("2026-Q3").unwrap(), Period::Quarter(2026, 3));
        assert_eq!(parse("2026-01").unwrap(), Period::Month(2026, 1));
        assert_eq!(parse("2026-M1").unwrap(), Period::Month(2026, 1));
        assert_eq!(parse("2026-W02").unwrap(), Period::Week(2026, 2));
        assert!(matches!(parse("last-30").unwrap(), Period::Rolling(30, _)));
        assert!(parse("garbage").is_err());
        assert!(parse("2026-Q5").is_err());
    }

    /// The defect this pins: an evening instant in a zone behind UTC is
    /// already the next UTC day, so a UTC-derived reference date resolves
    /// relative periods against a calendar the warehouse does not use.
    ///
    /// 2026-07-01T01:00:00Z is 2026-06-30 18:00 in Los Angeles. Every
    /// assertion below is the Los Angeles answer; the commented UTC answer is
    /// what the old code returned.
    #[test]
    fn relative_periods_resolve_in_the_target_zone_not_utc() {
        let evening_pdt = Utc.with_ymd_and_hms(2026, 7, 1, 1, 0, 0).unwrap();

        // Quarter boundary: the quarter that just ended, not the new one.
        assert_eq!(
            Period::parse_as_of("this-quarter", evening_pdt, Los_Angeles).unwrap(),
            Period::Quarter(2026, 2),
        );
        assert_eq!(
            Period::parse_as_of("previous-quarter", evening_pdt, Los_Angeles).unwrap(),
            Period::Quarter(2026, 1),
        );
        assert_eq!(
            Period::parse_as_of("this-month", evening_pdt, Los_Angeles).unwrap(),
            Period::Month(2026, 6),
        );

        // Rolling window: ends on the local day, not the UTC one.
        let rolling = Period::parse_as_of("last-30", evening_pdt, Los_Angeles).unwrap();
        let (start, end) = rolling.date_range();
        assert_eq!(end, NaiveDate::from_ymd_opt(2026, 6, 30).unwrap());
        assert_eq!(start, NaiveDate::from_ymd_opt(2026, 6, 1).unwrap());

        // For contrast, the UTC calendar gives the wrong quarter and a window
        // shifted a day forward — including a day still in progress locally.
        let utc_answer =
            Period::parse_with_reference("this-quarter", evening_pdt.date_naive()).unwrap();
        assert_eq!(utc_answer, Period::Quarter(2026, 3), "the old behavior");
    }

    /// Same rule under standard time, so the fix is a zone conversion rather
    /// than a fixed offset: 2026-01-01T01:00:00Z is 2025-12-31 17:00 PST.
    #[test]
    fn relative_periods_are_dst_aware_at_a_year_boundary() {
        let evening_pst = Utc.with_ymd_and_hms(2026, 1, 1, 1, 0, 0).unwrap();
        assert_eq!(
            Period::parse_as_of("this-year", evening_pst, Los_Angeles).unwrap(),
            Period::Year(2025),
        );
        assert_eq!(
            Period::parse_as_of("this-quarter", evening_pst, Los_Angeles).unwrap(),
            Period::Quarter(2025, 4),
        );
        assert_eq!(
            Period::parse_as_of("this-week", evening_pst, Los_Angeles).unwrap(),
            Period::Week(2026, 1),
            "ISO week 2026-W01 contains 2025-12-31",
        );
    }

    /// A zone ahead of UTC shifts the other way, so nothing here is hardcoded
    /// to "one day earlier": 2026-06-30T12:00:00Z is 2026-07-01 in Kiritimati.
    #[test]
    fn relative_periods_resolve_forward_in_an_eastern_zone() {
        let instant = Utc.with_ymd_and_hms(2026, 6, 30, 12, 0, 0).unwrap();
        assert_eq!(
            Period::parse_as_of("this-quarter", instant, Kiritimati).unwrap(),
            Period::Quarter(2026, 3),
        );
        assert_eq!(
            Period::parse_as_of("this-quarter", instant, Los_Angeles).unwrap(),
            Period::Quarter(2026, 2),
        );
    }

    #[test]
    fn relative_forms_resolve_against_reference() {
        let reference = NaiveDate::from_ymd_opt(2026, 7, 24).unwrap();
        assert_eq!(
            Period::parse_with_reference("this-quarter", reference).unwrap(),
            Period::Quarter(2026, 3)
        );
        assert_eq!(
            Period::parse_with_reference("previous-quarter", reference).unwrap(),
            Period::Quarter(2026, 2)
        );
        assert_eq!(
            Period::parse_with_reference("previous-month", reference).unwrap(),
            Period::Month(2026, 6)
        );
    }

    #[test]
    fn date_ranges_handle_leap_years_and_quarters() {
        let (start, end) = Period::Month(2024, 2).date_range();
        assert_eq!(start, NaiveDate::from_ymd_opt(2024, 2, 1).unwrap());
        assert_eq!(end, NaiveDate::from_ymd_opt(2024, 2, 29).unwrap());

        let (start, end) = Period::Quarter(2026, 4).date_range();
        assert_eq!(start, NaiveDate::from_ymd_opt(2026, 10, 1).unwrap());
        assert_eq!(end, NaiveDate::from_ymd_opt(2026, 12, 31).unwrap());

        let (start, end) = Period::Half(2026, 1).date_range();
        assert_eq!(start, NaiveDate::from_ymd_opt(2026, 1, 1).unwrap());
        assert_eq!(end, NaiveDate::from_ymd_opt(2026, 6, 30).unwrap());
    }

    #[test]
    fn previous_crosses_boundaries() {
        assert_eq!(
            Period::Quarter(2026, 1).previous(),
            Period::Quarter(2025, 4)
        );
        assert_eq!(Period::Month(2026, 1).previous(), Period::Month(2025, 12));
        assert_eq!(Period::Half(2026, 1).previous(), Period::Half(2025, 2));
        let rolling = Period::Rolling(30, NaiveDate::from_ymd_opt(2026, 7, 24).unwrap());
        let previous = rolling.previous();
        let (_, end) = previous.date_range();
        assert_eq!(end, NaiveDate::from_ymd_opt(2026, 6, 24).unwrap());
    }

    #[test]
    fn partial_period_machinery() {
        let quarter = Period::Quarter(2026, 3);
        let reference = NaiveDate::from_ymd_opt(2026, 7, 25).unwrap(); // day 25 of Q3
        assert!(quarter.is_partial(reference));
        assert_eq!(quarter.days_elapsed(reference), 25);
        assert_eq!(quarter.effective_end_date(reference), reference);
        assert_eq!(quarter.position_column(), Some("day_of_quarter"));

        let rolling = Period::Rolling(30, reference);
        assert!(!rolling.is_partial(reference));
        assert_eq!(rolling.position_column(), None);
    }

    #[test]
    fn column_selection_by_key() {
        assert_eq!(Period::column_for_key("2026-Q1"), "quarter_key");
        assert_eq!(Period::column_for_key("2026-W02"), "week_key");
        assert_eq!(Period::column_for_key("2026-H1"), "half_key");
        assert_eq!(Period::column_for_key("2026-01"), "month_key");
        assert_eq!(Period::column_for_key("2026"), "year_key");
    }
}
