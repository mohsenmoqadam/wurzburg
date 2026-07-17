use std::fmt;

use chrono::{DateTime, Utc};
use serde::Serialize;
use uuid::Uuid;

pub const DEFAULT_CARD_RANGE_PAGE_LIMIT: u16 = 50;
pub const MAX_CARD_RANGE_PAGE_LIMIT: u16 = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FundingMode {
    SingleProvider,
    MultiProvider,
}

impl FundingMode {
    pub fn as_db_value(self) -> &'static str {
        match self {
            Self::SingleProvider => "SINGLE_PROVIDER",
            Self::MultiProvider => "MULTI_PROVIDER",
        }
    }

    pub fn from_db_value(value: &str) -> Option<Self> {
        match value {
            "SINGLE_PROVIDER" => Some(Self::SingleProvider),
            "MULTI_PROVIDER" => Some(Self::MultiProvider),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WithdrawalLimitAuthority {
    Platform,
    Cms,
}

impl WithdrawalLimitAuthority {
    pub fn as_db_value(self) -> &'static str {
        match self {
            Self::Platform => "PLATFORM",
            Self::Cms => "CMS",
        }
    }

    pub fn from_db_value(value: &str) -> Option<Self> {
        match value {
            "PLATFORM" => Some(Self::Platform),
            "CMS" => Some(Self::Cms),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LimitWindowMode {
    Calendar,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WeekStartDay {
    Saturday,
    Sunday,
    Monday,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LimitCalendar {
    pub timezone: String,
    pub week_starts_on: WeekStartDay,
    pub window_mode: LimitWindowMode,
}

impl LimitCalendar {
    pub fn validate(&self) -> CardRangeResult<()> {
        if self.timezone.trim().is_empty() || self.timezone.chars().any(char::is_control) {
            return Err(CardRangeError::InvalidLimitCalendar(
                "timezone must be a non-empty IANA timezone string".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CardRangeStatus {
    Draft,
    Active,
    Suspended,
}

impl CardRangeStatus {
    pub fn as_db_value(self) -> &'static str {
        match self {
            Self::Draft => "DRAFT",
            Self::Active => "ACTIVE",
            Self::Suspended => "SUSPENDED",
        }
    }

    pub fn from_db_value(value: &str) -> Option<Self> {
        match value {
            "DRAFT" => Some(Self::Draft),
            "ACTIVE" => Some(Self::Active),
            "SUSPENDED" => Some(Self::Suspended),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CmsOperationMode {
    Full,
    BalanceOnly,
    Blocked,
}

impl CmsOperationMode {
    pub fn as_db_value(self) -> &'static str {
        match self {
            Self::Full => "FULL",
            Self::BalanceOnly => "BALANCE_ONLY",
            Self::Blocked => "BLOCKED",
        }
    }

    pub fn from_db_value(value: &str) -> Option<Self> {
        match value {
            "FULL" => Some(Self::Full),
            "BALANCE_ONLY" => Some(Self::BalanceOnly),
            "BLOCKED" => Some(Self::Blocked),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CardNumberRange {
    pub start: String,
    pub end: String,
}

impl CardNumberRange {
    pub fn new(start: impl AsRef<str>, end: impl AsRef<str>) -> CardRangeResult<Self> {
        let start = normalize_card_number(start.as_ref())?;
        let end = normalize_card_number(end.as_ref())?;

        if start > end {
            return Err(CardRangeError::InvalidBoundaryOrder);
        }

        Ok(Self { start, end })
    }

    pub fn overlaps(&self, other: &Self) -> bool {
        self.start <= other.end && self.end >= other.start
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewCardRange {
    pub card_range_id: Uuid,
    pub numbers: CardNumberRange,
    pub funding_mode: FundingMode,
    pub withdrawal_limit_authority: WithdrawalLimitAuthority,
    pub limit_calendar: Option<LimitCalendar>,
    pub issuance_enabled: bool,
    pub cms_operation_mode: CmsOperationMode,
    pub metadata_json: serde_json::Value,
}

impl NewCardRange {
    pub fn validate(&self) -> CardRangeResult<()> {
        validate_authority_calendar(
            self.withdrawal_limit_authority,
            self.limit_calendar.as_ref(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CardRange {
    pub card_range_id: Uuid,
    pub numbers: CardNumberRange,
    pub funding_mode: FundingMode,
    pub withdrawal_limit_authority: WithdrawalLimitAuthority,
    pub limit_calendar: Option<LimitCalendar>,
    pub status: CardRangeStatus,
    pub issuance_enabled: bool,
    pub cms_operation_mode: CmsOperationMode,
    pub operational_version: i64,
    pub metadata_json: serde_json::Value,
    pub created_by_subject: String,
    pub updated_by_subject: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl CardRange {
    pub fn replay_snapshot(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("CardRange serialization must be stable")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CardRangeListCursor {
    pub created_at: DateTime<Utc>,
    pub card_range_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CardRangeListQuery {
    pub status: Option<CardRangeStatus>,
    pub funding_mode: Option<FundingMode>,
    pub withdrawal_limit_authority: Option<WithdrawalLimitAuthority>,
    pub cursor: Option<CardRangeListCursor>,
    pub limit: u16,
}

impl CardRangeListQuery {
    pub fn new(
        status: Option<CardRangeStatus>,
        funding_mode: Option<FundingMode>,
        withdrawal_limit_authority: Option<WithdrawalLimitAuthority>,
        cursor: Option<CardRangeListCursor>,
        limit: Option<u16>,
    ) -> CardRangeResult<Self> {
        let limit = limit.unwrap_or(DEFAULT_CARD_RANGE_PAGE_LIMIT);
        if limit == 0 || limit > MAX_CARD_RANGE_PAGE_LIMIT {
            return Err(CardRangeError::InvalidPageLimit);
        }

        Ok(Self {
            status,
            funding_mode,
            withdrawal_limit_authority,
            cursor,
            limit,
        })
    }

    pub fn database_fetch_limit(&self) -> u16 {
        self.limit + 1
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CardRangeListPage {
    pub items: Vec<CardRange>,
    pub next_cursor: Option<CardRangeListCursor>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CardRangeProviderEligibility {
    pub card_range_id: Uuid,
    pub provider_id: Uuid,
    pub status: CardRangeProviderStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CardRangeProviderStatus {
    Active,
    Suspended,
}

impl CardRangeProviderStatus {
    pub fn as_db_value(self) -> &'static str {
        match self {
            Self::Active => "ACTIVE",
            Self::Suspended => "SUSPENDED",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CardRangeError {
    InvalidCardNumber,
    InvalidBoundaryOrder,
    InvalidLimitCalendar(String),
    InvalidPageLimit,
    PlatformAuthorityRequiresCalendar,
    CmsAuthorityRequiresNoCalendar,
}

impl fmt::Display for CardRangeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCardNumber => write!(
                formatter,
                "card number must be exactly 16 ASCII digits and must not start with zero"
            ),
            Self::InvalidBoundaryOrder => write!(
                formatter,
                "card range start must be less than or equal to end"
            ),
            Self::InvalidLimitCalendar(message) => {
                write!(formatter, "invalid limit calendar: {message}")
            }
            Self::InvalidPageLimit => write!(
                formatter,
                "card range page limit must be between 1 and {MAX_CARD_RANGE_PAGE_LIMIT}"
            ),
            Self::PlatformAuthorityRequiresCalendar => write!(
                formatter,
                "PLATFORM withdrawal authority requires a limit calendar"
            ),
            Self::CmsAuthorityRequiresNoCalendar => write!(
                formatter,
                "CMS withdrawal authority requires a null limit calendar"
            ),
        }
    }
}

impl std::error::Error for CardRangeError {}

pub type CardRangeResult<T> = Result<T, CardRangeError>;

pub fn normalize_card_number(value: &str) -> CardRangeResult<String> {
    let value = value.trim();
    if value.len() != 16
        || !value.bytes().all(|byte| byte.is_ascii_digit())
        || value.starts_with('0')
    {
        return Err(CardRangeError::InvalidCardNumber);
    }

    Ok(value.to_string())
}

pub fn validate_authority_calendar(
    authority: WithdrawalLimitAuthority,
    calendar: Option<&LimitCalendar>,
) -> CardRangeResult<()> {
    match (authority, calendar) {
        (WithdrawalLimitAuthority::Platform, Some(calendar)) => calendar.validate(),
        (WithdrawalLimitAuthority::Platform, None) => {
            Err(CardRangeError::PlatformAuthorityRequiresCalendar)
        }
        (WithdrawalLimitAuthority::Cms, Some(_)) => {
            Err(CardRangeError::CmsAuthorityRequiresNoCalendar)
        }
        (WithdrawalLimitAuthority::Cms, None) => Ok(()),
    }
}
