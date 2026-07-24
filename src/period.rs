//! The `Period` type: every time filter and metric window.

use chrono::{Datelike, Duration, NaiveDate, Utc};

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
    /// `last-N`, `this-*`, `previous-*`.
    pub fn parse(text: &str) -> Result<Self> {
        let today = Utc::now().date_naive();
        Self::parse_with_reference(text, today)
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

    #[test]
    fn parses_all_forms() {
        assert_eq!(Period::parse("2026").unwrap(), Period::Year(2026));
        assert_eq!(Period::parse("2026-H1").unwrap(), Period::Half(2026, 1));
        assert_eq!(Period::parse("2026-Q3").unwrap(), Period::Quarter(2026, 3));
        assert_eq!(Period::parse("2026-01").unwrap(), Period::Month(2026, 1));
        assert_eq!(Period::parse("2026-M1").unwrap(), Period::Month(2026, 1));
        assert_eq!(Period::parse("2026-W02").unwrap(), Period::Week(2026, 2));
        assert!(matches!(
            Period::parse("last-30").unwrap(),
            Period::Rolling(30, _)
        ));
        assert!(Period::parse("garbage").is_err());
        assert!(Period::parse("2026-Q5").is_err());
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
