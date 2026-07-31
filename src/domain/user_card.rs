use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum UserStatus {
    Active,
    Suspended,
}

impl UserStatus {
    pub fn as_db_value(self) -> &'static str {
        match self {
            Self::Active => "ACTIVE",
            Self::Suspended => "SUSPENDED",
        }
    }

    pub fn from_db_value(value: &str) -> Option<Self> {
        match value {
            "ACTIVE" => Some(Self::Active),
            "SUSPENDED" => Some(Self::Suspended),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProviderUserStatus {
    CardIssuancePending,
    Provisioning,
    Active,
    Suspended,
    IssuanceRejected,
    RecoveryRequired,
}

impl ProviderUserStatus {
    pub fn as_db_value(self) -> &'static str {
        match self {
            Self::CardIssuancePending => "CARD_ISSUANCE_PENDING",
            Self::Provisioning => "PROVISIONING",
            Self::Active => "ACTIVE",
            Self::Suspended => "SUSPENDED",
            Self::IssuanceRejected => "ISSUANCE_REJECTED",
            Self::RecoveryRequired => "RECOVERY_REQUIRED",
        }
    }

    pub fn from_db_value(value: &str) -> Option<Self> {
        match value {
            "CARD_ISSUANCE_PENDING" => Some(Self::CardIssuancePending),
            "PROVISIONING" => Some(Self::Provisioning),
            "ACTIVE" => Some(Self::Active),
            "SUSPENDED" => Some(Self::Suspended),
            "ISSUANCE_REJECTED" => Some(Self::IssuanceRejected),
            "RECOVERY_REQUIRED" => Some(Self::RecoveryRequired),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CardStatus {
    Provisioning,
    Active,
    Suspended,
    Replaced,
    Expired,
    RecoveryRequired,
}

impl CardStatus {
    pub fn as_db_value(self) -> &'static str {
        match self {
            Self::Provisioning => "PROVISIONING",
            Self::Active => "ACTIVE",
            Self::Suspended => "SUSPENDED",
            Self::Replaced => "REPLACED",
            Self::Expired => "EXPIRED",
            Self::RecoveryRequired => "RECOVERY_REQUIRED",
        }
    }

    pub fn from_db_value(value: &str) -> Option<Self> {
        match value {
            "PROVISIONING" => Some(Self::Provisioning),
            "ACTIVE" => Some(Self::Active),
            "SUSPENDED" => Some(Self::Suspended),
            "REPLACED" => Some(Self::Replaced),
            "EXPIRED" => Some(Self::Expired),
            "RECOVERY_REQUIRED" => Some(Self::RecoveryRequired),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CardResolution {
    ExistingCardAttached,
    IssuanceRequested,
    JoinedPendingIssuance,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CardInstruction {
    UseExisting { card_number: String },
    IssueNew(NewCardDelivery),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewCardDelivery {
    pub birth_date: Option<NaiveDate>,
    pub mobile: String,
    pub delivery_province: String,
    pub delivery_city: String,
    pub delivery_address: String,
    pub postal_code: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewProviderUserEnrollment {
    pub national_id: String,
    pub first_name: String,
    pub last_name: String,
    pub provider_customer_reference: String,
    pub selection_reference: String,
    pub card_instruction: CardInstruction,
    pub metadata: serde_json::Value,
}

impl NewProviderUserEnrollment {
    pub fn validate_and_normalize(mut self) -> Result<Self, &'static str> {
        self.national_id = normalize_digits(self.national_id.trim());
        if !is_valid_iranian_national_id(&self.national_id) {
            return Err("national ID is invalid");
        }
        self.first_name = normalize_required(self.first_name, 1, 255)?;
        self.last_name = normalize_required(self.last_name, 1, 255)?;
        self.provider_customer_reference =
            normalize_required(self.provider_customer_reference, 1, 255)?;
        self.selection_reference = normalize_required(self.selection_reference, 1, 255)?;
        if !self.metadata.is_object() {
            return Err("provider-user metadata must be a JSON object");
        }
        self.card_instruction = match self.card_instruction {
            CardInstruction::UseExisting { card_number } => {
                let card_number = normalize_digits(card_number.trim());
                if !is_valid_card_number(&card_number) {
                    return Err("existing card number is invalid");
                }
                CardInstruction::UseExisting { card_number }
            }
            CardInstruction::IssueNew(mut delivery) => {
                delivery.mobile = normalize_required(delivery.mobile, 10, 64)?;
                delivery.delivery_province =
                    normalize_required(delivery.delivery_province, 1, 255)?;
                delivery.delivery_city = normalize_required(delivery.delivery_city, 1, 255)?;
                delivery.delivery_address = normalize_required(delivery.delivery_address, 3, 2000)?;
                delivery.postal_code = normalize_digits(delivery.postal_code.trim());
                if delivery.postal_code.len() != 10
                    || !delivery
                        .postal_code
                        .bytes()
                        .all(|value| value.is_ascii_digit())
                {
                    return Err("postal code must contain exactly 10 digits");
                }
                CardInstruction::IssueNew(delivery)
            }
        };
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderUserView {
    pub enrollment_id: Uuid,
    pub provider_user_id: Uuid,
    pub provider_id: Uuid,
    pub user_id: Uuid,
    pub provider_customer_reference: String,
    pub identity_mismatch: bool,
    pub mismatch_fields: Vec<String>,
    pub status: ProviderUserStatus,
    pub card_resolution: CardResolution,
    pub card_id: Option<Uuid>,
    pub masked_card_number: Option<String>,
    pub card_range_id: Uuid,
    pub provider_user_account_id: Option<Uuid>,
    pub policy_usage_account_ids: Option<PolicyUsageAccountIds>,
    pub issuance_request_id: Option<Uuid>,
    pub profile_materialization_status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderUserRecord {
    pub provider_user_id: Uuid,
    pub enrollment_id: Uuid,
    pub provider_id: Uuid,
    pub user_id: Uuid,
    pub national_id: String,
    pub first_name: String,
    pub last_name: String,
    pub provider_customer_reference: String,
    pub identity_mismatch: bool,
    pub mismatch_fields: Vec<String>,
    pub status: ProviderUserStatus,
    pub card_id: Option<Uuid>,
    pub masked_card_number: Option<String>,
    pub card_range_id: Option<Uuid>,
    pub provider_user_account_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderUserCursor {
    pub created_at: DateTime<Utc>,
    pub provider_user_id: Uuid,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProviderUserPage {
    pub items: Vec<ProviderUserRecord>,
    pub next_cursor: Option<ProviderUserCursor>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserCardSummary {
    pub card_id: Uuid,
    pub masked_card_number: String,
    pub card_range_id: Uuid,
    pub status: String,
    pub state_version: i64,
    pub materialized_version: i64,
    pub provider_ids: Vec<Uuid>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserProviderSummary {
    pub provider_id: Uuid,
    pub provider_user_id: Uuid,
    pub provider_user_status: ProviderUserStatus,
    pub provider_user_account_id: Option<Uuid>,
    pub card_id: Option<Uuid>,
    pub masked_card_number: Option<String>,
    pub card_range_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
}

impl ProviderUserView {
    pub fn replay_snapshot(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("provider-user view must serialize")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyUsageAccountIds {
    pub amount_daily: Uuid,
    pub amount_weekly: Uuid,
    pub amount_monthly: Uuid,
    pub amount_yearly: Uuid,
    pub count_daily: Uuid,
    pub count_weekly: Uuid,
    pub count_monthly: Uuid,
    pub count_yearly: Uuid,
}

/// Complete, balance-free input used by Wolfsburg to rebuild one CP value.
///
/// The Kafka record key carries the PAN for same-card ordering. The durable
/// payload intentionally contains only internal identifiers and configuration;
/// Wolfsburg reads current balances from TigerBeetle before materialization.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CardProfileProjection {
    pub card_id: Uuid,
    pub user_id: Uuid,
    pub card_range_id: Uuid,
    pub funding_mode: String,
    pub state_version: i64,
    pub policy_usage_accounts: PolicyUsageAccountIds,
    pub funding_sources: Vec<CardProfileFundingSource>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CardProfileFundingSource {
    pub provider_id: Uuid,
    pub priority: u16,
    pub max_amount_rials: Option<u64>,
    pub ledger_accounts: CardProfileLedgerAccounts,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CardProfileLedgerAccounts {
    pub user_provider_account: Uuid,
    pub provider_fee_account: Uuid,
    pub cms_settlement_account: Uuid,
    pub platform_fee_account: Uuid,
}

impl PolicyUsageAccountIds {
    pub fn for_card(card_id: Uuid) -> Self {
        Self {
            amount_daily: PolicyUsageAccountCategory::AmountDaily.deterministic_id(card_id),
            amount_weekly: PolicyUsageAccountCategory::AmountWeekly.deterministic_id(card_id),
            amount_monthly: PolicyUsageAccountCategory::AmountMonthly.deterministic_id(card_id),
            amount_yearly: PolicyUsageAccountCategory::AmountYearly.deterministic_id(card_id),
            count_daily: PolicyUsageAccountCategory::CountDaily.deterministic_id(card_id),
            count_weekly: PolicyUsageAccountCategory::CountWeekly.deterministic_id(card_id),
            count_monthly: PolicyUsageAccountCategory::CountMonthly.deterministic_id(card_id),
            count_yearly: PolicyUsageAccountCategory::CountYearly.deterministic_id(card_id),
        }
    }

    pub fn ordered(&self) -> [Uuid; 8] {
        [
            self.amount_daily,
            self.amount_weekly,
            self.amount_monthly,
            self.amount_yearly,
            self.count_daily,
            self.count_weekly,
            self.count_monthly,
            self.count_yearly,
        ]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyUsageAccountCategory {
    AmountDaily,
    AmountWeekly,
    AmountMonthly,
    AmountYearly,
    CountDaily,
    CountWeekly,
    CountMonthly,
    CountYearly,
}

impl PolicyUsageAccountCategory {
    pub const ALL: [Self; 8] = [
        Self::AmountDaily,
        Self::AmountWeekly,
        Self::AmountMonthly,
        Self::AmountYearly,
        Self::CountDaily,
        Self::CountWeekly,
        Self::CountMonthly,
        Self::CountYearly,
    ];

    pub fn code(self) -> u32 {
        match self {
            Self::AmountDaily => 1,
            Self::AmountWeekly => 2,
            Self::AmountMonthly => 3,
            Self::AmountYearly => 4,
            Self::CountDaily => 5,
            Self::CountWeekly => 6,
            Self::CountMonthly => 7,
            Self::CountYearly => 8,
        }
    }

    pub fn deterministic_id(self, card_id: Uuid) -> Uuid {
        deterministic_uuid(
            b"wurzburg.card.policy-usage-account.v1",
            &[card_id.as_bytes(), &self.code().to_be_bytes()],
        )
    }
}

pub fn deterministic_provider_user_account_id(provider_id: Uuid, user_id: Uuid) -> Uuid {
    deterministic_uuid(
        b"wurzburg.provider-user.account.v1",
        &[provider_id.as_bytes(), user_id.as_bytes()],
    )
}

pub fn mask_card_number(card_number: &str) -> String {
    if card_number.len() != 16 {
        return "****".to_string();
    }
    format!("{}********{}", &card_number[..4], &card_number[12..])
}

pub fn is_valid_card_number(value: &str) -> bool {
    value.len() == 16 && !value.starts_with('0') && value.bytes().all(|byte| byte.is_ascii_digit())
}

pub fn is_valid_iranian_national_id(value: &str) -> bool {
    if value.len() != 10 || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return false;
    }
    let digits = value
        .bytes()
        .map(|byte| u32::from(byte - b'0'))
        .collect::<Vec<_>>();
    if digits.iter().all(|digit| *digit == digits[0]) {
        return false;
    }
    let weighted_sum: u32 = digits[..9]
        .iter()
        .enumerate()
        .map(|(index, digit)| digit * (10 - index as u32))
        .sum();
    let remainder = weighted_sum % 11;
    let expected = if remainder < 2 {
        remainder
    } else {
        11 - remainder
    };
    digits[9] == expected
}

fn deterministic_uuid(namespace: &[u8], parts: &[&[u8]]) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(namespace);
    for part in parts {
        hasher.update(part);
    }
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn normalize_required(value: String, min: usize, max: usize) -> Result<String, &'static str> {
    let value = normalize_digits(value.trim());
    if value.chars().count() < min
        || value.chars().count() > max
        || value.chars().any(char::is_control)
    {
        return Err("user/card text field has an invalid length or character");
    }
    Ok(value)
}

pub fn normalize_digits(value: &str) -> String {
    value
        .chars()
        .map(|character| match character {
            '\u{06f0}' | '\u{0660}' => '0',
            '\u{06f1}' | '\u{0661}' => '1',
            '\u{06f2}' | '\u{0662}' => '2',
            '\u{06f3}' | '\u{0663}' => '3',
            '\u{06f4}' | '\u{0664}' => '4',
            '\u{06f5}' | '\u{0665}' => '5',
            '\u{06f6}' | '\u{0666}' => '6',
            '\u{06f7}' | '\u{0667}' => '7',
            '\u{06f8}' | '\u{0668}' => '8',
            '\u{06f9}' | '\u{0669}' => '9',
            value => value,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_iranian_national_id_and_rejects_repeated_digits() {
        assert!(is_valid_iranian_national_id("0013547852"));
        assert!(!is_valid_iranian_national_id("0013547858"));
        assert!(!is_valid_iranian_national_id("1111111111"));
    }

    #[test]
    fn deterministic_accounts_are_stable_and_card_scoped() {
        let card_a = Uuid::new_v4();
        let card_b = Uuid::new_v4();
        assert_eq!(
            PolicyUsageAccountIds::for_card(card_a),
            PolicyUsageAccountIds::for_card(card_a)
        );
        assert_ne!(
            PolicyUsageAccountIds::for_card(card_a),
            PolicyUsageAccountIds::for_card(card_b)
        );
    }

    #[test]
    fn existing_card_instruction_requires_an_exact_valid_pan() {
        let enrollment = valid_enrollment(CardInstruction::UseExisting {
            card_number: "6219861000000001".to_string(),
        });
        assert!(enrollment.validate_and_normalize().is_ok());

        let invalid = valid_enrollment(CardInstruction::UseExisting {
            card_number: "6219861".to_string(),
        });
        assert!(invalid.validate_and_normalize().is_err());
    }

    #[test]
    fn new_card_instruction_requires_complete_delivery_identity() {
        let enrollment = valid_enrollment(CardInstruction::IssueNew(NewCardDelivery {
            birth_date: None,
            mobile: "09120000000".to_string(),
            delivery_province: "Tehran".to_string(),
            delivery_city: "Tehran".to_string(),
            delivery_address: "Delivery address".to_string(),
            postal_code: "1234567890".to_string(),
        }));
        assert!(enrollment.validate_and_normalize().is_ok());

        let invalid = valid_enrollment(CardInstruction::IssueNew(NewCardDelivery {
            birth_date: None,
            mobile: "09120000000".to_string(),
            delivery_province: "Tehran".to_string(),
            delivery_city: "Tehran".to_string(),
            delivery_address: "Delivery address".to_string(),
            postal_code: "123".to_string(),
        }));
        assert!(invalid.validate_and_normalize().is_err());
    }

    fn valid_enrollment(card_instruction: CardInstruction) -> NewProviderUserEnrollment {
        NewProviderUserEnrollment {
            national_id: "0013547852".to_string(),
            first_name: "First".to_string(),
            last_name: "Last".to_string(),
            provider_customer_reference: "provider-customer-1".to_string(),
            selection_reference: "selection-1".to_string(),
            card_instruction,
            metadata: serde_json::json!({}),
        }
    }
}
