use chrono::{DateTime, NaiveTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProviderStatus {
    PendingProvisioning,
    Ready,
    Active,
    Suspended,
    Inactive,
    Failed,
}

impl ProviderStatus {
    pub fn as_db_value(self) -> &'static str {
        match self {
            Self::PendingProvisioning => "PENDING_PROVISIONING",
            Self::Ready => "READY",
            Self::Active => "ACTIVE",
            Self::Suspended => "SUSPENDED",
            Self::Inactive => "INACTIVE",
            Self::Failed => "FAILED",
        }
    }

    pub fn from_db_value(value: &str) -> Option<Self> {
        match value {
            "PENDING_PROVISIONING" => Some(Self::PendingProvisioning),
            "READY" => Some(Self::Ready),
            "ACTIVE" => Some(Self::Active),
            "SUSPENDED" => Some(Self::Suspended),
            "INACTIVE" => Some(Self::Inactive),
            "FAILED" => Some(Self::Failed),
            _ => None,
        }
    }

    pub fn can_transition_to(self, target: Self) -> bool {
        matches!(
            (self, target),
            (Self::PendingProvisioning, Self::Ready | Self::Failed)
                | (Self::Failed, Self::PendingProvisioning)
                | (Self::Ready, Self::Active)
                | (Self::Active, Self::Suspended | Self::Inactive)
                | (Self::Suspended, Self::Active | Self::Inactive)
                | (Self::Inactive, Self::Active)
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProviderAccountCategory {
    ProviderOwned,
    ProviderFee,
    CmsSettlement,
    PlatformFee,
}

impl ProviderAccountCategory {
    pub const ALL: [Self; 4] = [
        Self::ProviderOwned,
        Self::ProviderFee,
        Self::CmsSettlement,
        Self::PlatformFee,
    ];

    pub fn as_db_value(self) -> &'static str {
        match self {
            Self::ProviderOwned => "PROVIDER_OWNED",
            Self::ProviderFee => "PROVIDER_FEE",
            Self::CmsSettlement => "CMS_SETTLEMENT",
            Self::PlatformFee => "PLATFORM_FEE",
        }
    }

    pub fn from_db_value(value: &str) -> Option<Self> {
        match value {
            "PROVIDER_OWNED" => Some(Self::ProviderOwned),
            "PROVIDER_FEE" => Some(Self::ProviderFee),
            "CMS_SETTLEMENT" => Some(Self::CmsSettlement),
            "PLATFORM_FEE" => Some(Self::PlatformFee),
            _ => None,
        }
    }

    pub fn deterministic_account_id(self, provider_id: Uuid) -> Uuid {
        let mut hasher = Sha256::new();
        hasher.update(b"wurzburg.provider.account.v1");
        hasher.update(provider_id.as_bytes());
        hasher.update(self.as_db_value().as_bytes());
        let digest = hasher.finalize();
        let mut bytes = [0_u8; 16];
        bytes.copy_from_slice(&digest[..16]);
        bytes[6] = (bytes[6] & 0x0f) | 0x50;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Uuid::from_bytes(bytes)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderContact {
    pub provider_contact_id: Uuid,
    pub contact_type: ProviderContactType,
    pub name: Option<String>,
    pub email: Option<String>,
    pub phone: Option<String>,
    pub mobile: Option<String>,
    pub sms_enabled: bool,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProviderContactType {
    Finance,
    Technical,
    Operations,
    Security,
    Notification,
    Legal,
}

impl ProviderContactType {
    pub fn as_db_value(self) -> &'static str {
        match self {
            Self::Finance => "FINANCE",
            Self::Technical => "TECHNICAL",
            Self::Operations => "OPERATIONS",
            Self::Security => "SECURITY",
            Self::Notification => "NOTIFICATION",
            Self::Legal => "LEGAL",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderOperationalProfile {
    pub effective_at: DateTime<Utc>,
    pub timezone: String,
    pub user_onboarding_enabled: bool,
    pub active_windows: Vec<ProviderActiveWindow>,
    pub max_total_users: Option<u64>,
    pub credit_grant_enabled: bool,
    pub credit_grant_mode: CreditGrantLimitMode,
    pub credit_grant_limit_amount_rials: u128,
    pub credit_return_enabled: bool,
    pub new_assignment_enabled: bool,
    pub same_pan_reprint_enabled: bool,
    pub new_pan_replacement_enabled: bool,
    pub attach_existing_multi_provider_card_enabled: bool,
    pub event_delivery_enabled: bool,
    pub event_delivery_disabled_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderActiveWindow {
    pub days: Vec<ProviderWeekday>,
    pub start_local_time: NaiveTime,
    pub end_local_time: NaiveTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProviderWeekday {
    Saturday,
    Sunday,
    Monday,
    Tuesday,
    Wednesday,
    Thursday,
    Friday,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CreditGrantLimitMode {
    FixedLimit,
    CmsDebtLimit,
    OutstandingCreditLimit,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewProvider {
    pub provider_id: Uuid,
    pub legal_name: String,
    pub trade_name: String,
    pub tax_id: Option<String>,
    pub registration_number: Option<String>,
    pub email_address: Option<String>,
    pub website_url: Option<String>,
    pub mailing_address: Option<String>,
    pub metadata: serde_json::Value,
    pub contacts: Vec<ProviderContact>,
    pub operational_profile: ProviderOperationalProfile,
}

impl NewProvider {
    pub fn validate_and_normalize(mut self) -> Result<Self, &'static str> {
        self.legal_name = normalize_required(self.legal_name, 2, 255)?;
        self.trade_name = normalize_required(self.trade_name, 2, 255)?;
        self.tax_id = normalize_optional(self.tax_id, 64)?;
        self.registration_number = normalize_optional(self.registration_number, 128)?;
        self.email_address = normalize_optional(self.email_address, 255)?;
        self.website_url = normalize_optional(self.website_url, 512)?;
        self.mailing_address = normalize_optional(self.mailing_address, 2000)?;
        if !self.metadata.is_object() {
            return Err("provider metadata must be a JSON object");
        }
        if self.operational_profile.timezone.trim().is_empty() {
            return Err("operational profile timezone is required");
        }
        for window in &self.operational_profile.active_windows {
            if window.days.is_empty() || window.start_local_time >= window.end_local_time {
                return Err("operational windows require days and must not cross midnight");
            }
        }
        for (index, left) in self.operational_profile.active_windows.iter().enumerate() {
            for right in self
                .operational_profile
                .active_windows
                .iter()
                .skip(index + 1)
            {
                let same_day = left.days.iter().any(|day| right.days.contains(day));
                let overlaps = left.start_local_time < right.end_local_time
                    && right.start_local_time < left.end_local_time;
                if same_day && overlaps {
                    return Err("operational windows must not overlap on the same day");
                }
            }
        }
        if self.operational_profile.event_delivery_enabled
            && self
                .operational_profile
                .event_delivery_disabled_reason
                .is_some()
        {
            return Err("enabled event delivery cannot have a disabled reason");
        }
        if !self.operational_profile.event_delivery_enabled
            && self
                .operational_profile
                .event_delivery_disabled_reason
                .as_deref()
                .is_none_or(|reason| reason.trim().is_empty())
        {
            return Err("disabled event delivery requires a reason");
        }
        for contact in &mut self.contacts {
            if !contact.metadata.is_object() {
                return Err("contact metadata must be a JSON object");
            }
            contact.name = normalize_optional(contact.name.take(), 255)?;
            contact.email = normalize_optional(contact.email.take(), 255)?;
            contact.phone = normalize_optional(contact.phone.take(), 64)?;
            contact.mobile = normalize_optional(contact.mobile.take(), 64)?;
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Provider {
    pub provider_id: Uuid,
    pub legal_name: String,
    pub trade_name: String,
    pub tax_id: Option<String>,
    pub registration_number: Option<String>,
    pub email_address: Option<String>,
    pub website_url: Option<String>,
    pub mailing_address: Option<String>,
    pub status: ProviderStatus,
    pub metadata: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Provider {
    pub fn replay_snapshot(&self) -> serde_json::Value {
        let core_provisioning_status = match self.status {
            ProviderStatus::PendingProvisioning => "PENDING",
            ProviderStatus::Failed => "FAILED",
            ProviderStatus::Ready
            | ProviderStatus::Active
            | ProviderStatus::Suspended
            | ProviderStatus::Inactive => "SUCCEEDED",
        };
        serde_json::json!({
            "provider_id": self.provider_id,
            "legal_name": self.legal_name,
            "trade_name": self.trade_name,
            "tax_id": self.tax_id,
            "registration_number": self.registration_number,
            "email_address": self.email_address,
            "website_url": self.website_url,
            "mailing_address": self.mailing_address,
            "status": self.status,
            "metadata": self.metadata,
            "core_provisioning_status": core_provisioning_status,
            "kafka_provisioning_status": "PENDING",
            "created_at": self.created_at,
            "updated_at": self.updated_at
        })
    }
}

fn normalize_required(value: String, min: usize, max: usize) -> Result<String, &'static str> {
    let value = normalize_digits(value.trim());
    if value.len() < min || value.len() > max || value.chars().any(char::is_control) {
        return Err("provider text field has an invalid length or character");
    }
    Ok(value)
}

fn normalize_optional(value: Option<String>, max: usize) -> Result<Option<String>, &'static str> {
    value
        .map(|value| {
            let value = normalize_digits(value.trim());
            if value.is_empty() {
                return Ok(None);
            }
            if value.len() > max || value.chars().any(char::is_control) {
                return Err("provider optional field has an invalid length or character");
            }
            Ok(Some(value))
        })
        .unwrap_or(Ok(None))
}

fn normalize_digits(value: &str) -> String {
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
            other => other,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{ProviderAccountCategory, ProviderStatus};
    use uuid::Uuid;

    #[test]
    fn deterministic_provider_accounts_are_stable_and_category_specific() {
        let provider_id = Uuid::parse_str("4f3c2e1a-0b9d-4c7e-8f6a-123456789abc").unwrap();
        let first = ProviderAccountCategory::ProviderOwned.deterministic_account_id(provider_id);
        let replay = ProviderAccountCategory::ProviderOwned.deterministic_account_id(provider_id);
        let fee = ProviderAccountCategory::ProviderFee.deterministic_account_id(provider_id);
        assert_eq!(first, replay);
        assert_ne!(first, fee);
    }

    #[test]
    fn lifecycle_rejects_activation_before_core_provisioning() {
        assert!(!ProviderStatus::PendingProvisioning.can_transition_to(ProviderStatus::Active));
        assert!(ProviderStatus::PendingProvisioning.can_transition_to(ProviderStatus::Ready));
        assert!(ProviderStatus::Ready.can_transition_to(ProviderStatus::Active));
    }
}
