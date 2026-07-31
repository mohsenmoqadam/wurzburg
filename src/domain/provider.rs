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

    pub fn from_db_value(value: &str) -> Option<Self> {
        match value {
            "FINANCE" => Some(Self::Finance),
            "TECHNICAL" => Some(Self::Technical),
            "OPERATIONS" => Some(Self::Operations),
            "SECURITY" => Some(Self::Security),
            "NOTIFICATION" => Some(Self::Notification),
            "LEGAL" => Some(Self::Legal),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProviderContactStatus {
    Active,
    Suspended,
}

impl ProviderContactStatus {
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderContactRecord {
    pub provider_contact_id: Uuid,
    pub provider_id: Uuid,
    pub contact_type: ProviderContactType,
    pub name: Option<String>,
    pub email: Option<String>,
    pub phone: Option<String>,
    pub mobile: Option<String>,
    pub sms_enabled: bool,
    pub metadata: serde_json::Value,
    pub status: ProviderContactStatus,
    pub created_by_subject: String,
    pub updated_by_subject: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ProviderContactRecord {
    pub fn replay_snapshot(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("provider contact record is serializable")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderContactCursor {
    pub created_at: DateTime<Utc>,
    pub provider_contact_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderContactListQuery {
    pub contact_type: Option<ProviderContactType>,
    pub status: Option<ProviderContactStatus>,
    pub limit: u32,
    pub cursor: Option<ProviderContactCursor>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProviderContactListPage {
    pub items: Vec<ProviderContactRecord>,
    pub next_cursor: Option<ProviderContactCursor>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FieldUpdate<T> {
    Unchanged,
    Set(T),
    Clear,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProviderIdentityUpdate {
    pub legal_name: Option<String>,
    pub trade_name: Option<String>,
    pub tax_id: FieldUpdate<String>,
    pub registration_number: FieldUpdate<String>,
    pub email_address: FieldUpdate<String>,
    pub website_url: FieldUpdate<String>,
    pub mailing_address: FieldUpdate<String>,
    pub metadata: Option<serde_json::Value>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProviderContactUpdate {
    pub contact_type: Option<ProviderContactType>,
    pub name: FieldUpdate<String>,
    pub email: FieldUpdate<String>,
    pub phone: FieldUpdate<String>,
    pub mobile: FieldUpdate<String>,
    pub sms_enabled: Option<bool>,
    pub metadata: Option<serde_json::Value>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderOperationalProfile {
    pub effective_at: DateTime<Utc>,
    #[serde(flatten)]
    pub controls: ProviderOperationalControls,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderOperationalControls {
    pub timezone: String,
    pub user_onboarding_enabled: bool,
    pub active_windows: Vec<ProviderActiveWindow>,
    pub max_total_users: Option<u64>,
    pub credit_grant_enabled: bool,
    pub credit_grant_mode: CreditGrantLimitMode,
    pub credit_grant_limit_amount_rials: u64,
    pub credit_return_enabled: bool,
    pub new_assignment_enabled: bool,
    pub same_pan_reprint_enabled: bool,
    pub new_pan_replacement_enabled: bool,
    pub attach_existing_multi_provider_card_enabled: bool,
    pub event_delivery_enabled: bool,
    pub event_delivery_disabled_reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProviderOperationalProfileStatus {
    Scheduled,
    Active,
    Superseded,
    Cancelled,
}

impl ProviderOperationalProfileStatus {
    pub fn as_db_value(self) -> &'static str {
        match self {
            Self::Scheduled => "SCHEDULED",
            Self::Active => "ACTIVE",
            Self::Superseded => "SUPERSEDED",
            Self::Cancelled => "CANCELLED",
        }
    }

    pub fn from_db_value(value: &str) -> Option<Self> {
        match value {
            "SCHEDULED" => Some(Self::Scheduled),
            "ACTIVE" => Some(Self::Active),
            "SUPERSEDED" => Some(Self::Superseded),
            "CANCELLED" => Some(Self::Cancelled),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderOperationalProfileRecord {
    pub provider_operational_profile_id: Uuid,
    pub provider_id: Uuid,
    pub status: ProviderOperationalProfileStatus,
    pub version: i64,
    pub effective_at: DateTime<Utc>,
    pub controls: ProviderOperationalControls,
    pub superseded_by_profile_id: Option<Uuid>,
    pub created_by_subject: String,
    pub updated_by_subject: String,
    pub change_reason: String,
    pub activated_at: Option<DateTime<Utc>>,
    pub superseded_at: Option<DateTime<Utc>>,
    pub cancelled_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ProviderOperationalProfileRecord {
    pub fn replay_snapshot(&self) -> serde_json::Value {
        serde_json::json!({
            "provider_operational_profile_id": self.provider_operational_profile_id,
            "provider_id": self.provider_id,
            "status": self.status,
            "version": self.version,
            "effective_at": self.effective_at,
            "profile": {
                "timezone": self.controls.timezone,
                "user_onboarding": {
                    "enabled": self.controls.user_onboarding_enabled,
                    "active_windows": self.controls.active_windows,
                    "max_total_users": self.controls.max_total_users,
                },
                "credit_grant": {
                    "enabled": self.controls.credit_grant_enabled,
                    "mode": self.controls.credit_grant_mode.as_api_value(),
                    "limit_amount_rials": self.controls.credit_grant_limit_amount_rials,
                },
                "credit_return": { "enabled": self.controls.credit_return_enabled },
                "card_operations": {
                    "new_assignment_enabled": self.controls.new_assignment_enabled,
                    "same_pan_reprint_enabled": self.controls.same_pan_reprint_enabled,
                    "new_pan_replacement_enabled": self.controls.new_pan_replacement_enabled,
                    "attach_existing_multi_provider_card_enabled": self.controls.attach_existing_multi_provider_card_enabled,
                },
                "event_delivery": {
                    "enabled": self.controls.event_delivery_enabled,
                    "disabled_reason": self.controls.event_delivery_disabled_reason,
                },
            },
            "superseded_by_profile_id": self.superseded_by_profile_id,
            "created_by_subject": self.created_by_subject,
            "updated_by_subject": self.updated_by_subject,
            "change_reason": self.change_reason,
            "activated_at": self.activated_at,
            "superseded_at": self.superseded_at,
            "cancelled_at": self.cancelled_at,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DesiredProviderOperationalProfile {
    pub effective_at: DateTime<Utc>,
    pub controls: ProviderOperationalControls,
    pub reason: String,
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

impl CreditGrantLimitMode {
    pub fn as_api_value(self) -> &'static str {
        match self {
            Self::FixedLimit => "FixedLimit",
            Self::CmsDebtLimit => "CmsDebtLimit",
            Self::OutstandingCreditLimit => "OutstandingCreditLimit",
        }
    }
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
    pub kafka_access: Option<NewProviderKafkaAccess>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewProviderKafkaAccess {
    pub provider_kafka_access_id: Uuid,
    pub provider_kafka_credential_id: Uuid,
    pub topic_name: String,
    pub username: String,
    pub consumer_group: String,
    pub password_ciphertext: String,
    pub encryption_key_version: String,
    pub credential_version: u64,
    pub security_protocol: String,
    pub sasl_mechanism: String,
    pub bootstrap_servers: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewProviderKafkaCredential {
    pub provider_kafka_credential_id: Uuid,
    pub password_ciphertext: String,
    pub encryption_key_version: String,
    pub credential_version: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProviderKafkaProvisioningStatus {
    Disabled,
    Pending,
    Succeeded,
    Failed,
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
        self.operational_profile.controls.validate()?;
        for contact in &mut self.contacts {
            contact.validate_and_normalize()?;
        }
        Ok(self)
    }
}

impl ProviderContact {
    pub fn validate_and_normalize(&mut self) -> Result<(), &'static str> {
        if !self.metadata.is_object() {
            return Err("contact metadata must be a JSON object");
        }
        self.name = normalize_optional(self.name.take(), 255)?;
        self.email = normalize_optional(self.email.take(), 255)?;
        self.phone = normalize_optional(self.phone.take(), 64)?;
        self.mobile = normalize_optional(self.mobile.take(), 64)?;
        if self.name.is_none()
            && self.email.is_none()
            && self.phone.is_none()
            && self.mobile.is_none()
        {
            return Err("contact must contain at least one name or communication channel");
        }
        if self.sms_enabled && self.mobile.is_none() {
            return Err("SMS-enabled contact requires a mobile number");
        }
        Ok(())
    }
}

impl ProviderIdentityUpdate {
    pub fn validate_and_normalize(mut self) -> Result<Self, &'static str> {
        self.legal_name = self
            .legal_name
            .map(|value| normalize_required(value, 2, 255))
            .transpose()?;
        self.trade_name = self
            .trade_name
            .map(|value| normalize_required(value, 2, 255))
            .transpose()?;
        self.tax_id = normalize_field_update(self.tax_id, 64)?;
        self.registration_number = normalize_field_update(self.registration_number, 128)?;
        self.email_address = normalize_field_update(self.email_address, 255)?;
        self.website_url = normalize_field_update(self.website_url, 512)?;
        self.mailing_address = normalize_field_update(self.mailing_address, 2000)?;
        if self
            .metadata
            .as_ref()
            .is_some_and(|value| !value.is_object())
        {
            return Err("provider metadata must be a JSON object");
        }
        self.reason = normalize_reason(self.reason, "provider identity change reason")?;
        if self.legal_name.is_none()
            && self.trade_name.is_none()
            && matches!(self.tax_id, FieldUpdate::Unchanged)
            && matches!(self.registration_number, FieldUpdate::Unchanged)
            && matches!(self.email_address, FieldUpdate::Unchanged)
            && matches!(self.website_url, FieldUpdate::Unchanged)
            && matches!(self.mailing_address, FieldUpdate::Unchanged)
            && self.metadata.is_none()
        {
            return Err("provider identity update must change at least one field");
        }
        Ok(self)
    }
}

impl ProviderContactUpdate {
    pub fn validate_and_normalize(mut self) -> Result<Self, &'static str> {
        self.name = normalize_field_update(self.name, 255)?;
        self.email = normalize_field_update(self.email, 255)?;
        self.phone = normalize_field_update(self.phone, 64)?;
        self.mobile = normalize_field_update(self.mobile, 64)?;
        if self
            .metadata
            .as_ref()
            .is_some_and(|value| !value.is_object())
        {
            return Err("contact metadata must be a JSON object");
        }
        self.reason = normalize_reason(self.reason, "provider contact change reason")?;
        if self.contact_type.is_none()
            && matches!(self.name, FieldUpdate::Unchanged)
            && matches!(self.email, FieldUpdate::Unchanged)
            && matches!(self.phone, FieldUpdate::Unchanged)
            && matches!(self.mobile, FieldUpdate::Unchanged)
            && self.sms_enabled.is_none()
            && self.metadata.is_none()
        {
            return Err("provider contact update must change at least one field");
        }
        Ok(self)
    }
}

impl ProviderOperationalControls {
    pub fn validate(&mut self) -> Result<(), &'static str> {
        self.timezone = self.timezone.trim().to_string();
        if self.timezone.is_empty() || self.timezone.parse::<chrono_tz::Tz>().is_err() {
            return Err("operational profile timezone must be a valid IANA timezone");
        }
        for window in &self.active_windows {
            if window.days.is_empty() || window.start_local_time >= window.end_local_time {
                return Err("operational windows require days and must not cross midnight");
            }
        }
        for (index, left) in self.active_windows.iter().enumerate() {
            for right in self.active_windows.iter().skip(index + 1) {
                let same_day = left.days.iter().any(|day| right.days.contains(day));
                let overlaps = left.start_local_time < right.end_local_time
                    && right.start_local_time < left.end_local_time;
                if same_day && overlaps {
                    return Err("operational windows must not overlap on the same day");
                }
            }
        }
        if self.event_delivery_enabled && self.event_delivery_disabled_reason.is_some() {
            return Err("enabled event delivery cannot have a disabled reason");
        }
        if !self.event_delivery_enabled
            && self
                .event_delivery_disabled_reason
                .as_deref()
                .is_none_or(|reason| reason.trim().is_empty())
        {
            return Err("disabled event delivery requires a reason");
        }
        self.event_delivery_disabled_reason = self
            .event_delivery_disabled_reason
            .take()
            .map(|value| value.trim().to_string());
        Ok(())
    }
}

impl DesiredProviderOperationalProfile {
    pub fn validate_and_normalize(mut self) -> Result<Self, &'static str> {
        self.controls.validate()?;
        self.reason = self.reason.trim().to_string();
        if self.reason.is_empty() || self.reason.chars().count() > 1000 {
            return Err("operational profile change reason must contain 1 to 1000 characters");
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
    pub kafka_provisioning_status: ProviderKafkaProvisioningStatus,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderListItem {
    pub provider_id: Uuid,
    pub legal_name: String,
    pub trade_name: String,
    pub tax_id: Option<String>,
    pub status: ProviderStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderListCursor {
    pub created_at: DateTime<Utc>,
    pub provider_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderListQuery {
    pub status: Option<ProviderStatus>,
    pub tax_id: Option<String>,
    pub limit: u32,
    pub cursor: Option<ProviderListCursor>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProviderListPage {
    pub items: Vec<ProviderListItem>,
    pub next_cursor: Option<ProviderListCursor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderLedgerAccountBalance {
    pub account_category: ProviderAccountCategory,
    pub tigerbeetle_account_id: Uuid,
    pub debits_posted: String,
    pub credits_posted: String,
    pub debits_pending: String,
    pub credits_pending: String,
    pub posted_balance: String,
    pub effective_balance: String,
    pub status: String,
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
            "kafka_provisioning_status": self.kafka_provisioning_status,
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

fn normalize_field_update(
    value: FieldUpdate<String>,
    max: usize,
) -> Result<FieldUpdate<String>, &'static str> {
    match value {
        FieldUpdate::Unchanged => Ok(FieldUpdate::Unchanged),
        FieldUpdate::Clear => Ok(FieldUpdate::Clear),
        FieldUpdate::Set(value) => normalize_optional(Some(value), max)?.map_or_else(
            || Ok(FieldUpdate::Clear),
            |value| Ok(FieldUpdate::Set(value)),
        ),
    }
}

pub fn normalize_reason(value: String, label: &'static str) -> Result<String, &'static str> {
    let value = value.trim().to_string();
    if value.is_empty() || value.chars().count() > 1000 || value.chars().any(char::is_control) {
        return Err(label);
    }
    Ok(value)
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
