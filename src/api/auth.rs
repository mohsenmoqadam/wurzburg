use std::collections::BTreeSet;

use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

use crate::{
    api::{
        error::ApiError, request_context::TrustedRequestContext, result_codes::WurzburgResultCode,
    },
    config::Wso2Config,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedActorClaims {
    pub issuer: String,
    pub subject: String,
    pub client_id: String,
    pub roles: Vec<String>,
    pub scopes: Vec<String>,
    pub provider_id: Option<Uuid>,
    pub user_id: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrustedActor {
    pub issuer: String,
    pub subject: String,
    pub client_id: String,
    roles: BTreeSet<String>,
    scopes: BTreeSet<String>,
    pub provider_id: Option<Uuid>,
    pub user_id: Option<Uuid>,
}

impl TrustedActor {
    pub fn from_verified_claims(claims: VerifiedActorClaims) -> Result<Self, ApiError> {
        if claims.issuer.trim().is_empty()
            || claims.subject.trim().is_empty()
            || claims.client_id.trim().is_empty()
        {
            return Err(ApiError::new(WurzburgResultCode::InvalidActorClaim));
        }

        Ok(Self {
            issuer: claims.issuer,
            subject: claims.subject,
            client_id: claims.client_id,
            roles: normalize_exact_claims(claims.roles)?,
            scopes: normalize_exact_claims(claims.scopes)?,
            provider_id: claims.provider_id,
            user_id: claims.user_id,
        })
    }

    pub fn has_scope(&self, scope: &str) -> bool {
        self.scopes.contains(scope)
    }

    pub fn has_role(&self, role: &str) -> bool {
        self.roles.contains(role)
    }

    pub fn scopes(&self) -> impl Iterator<Item = &str> {
        self.scopes.iter().map(String::as_str)
    }
}

pub fn require_scope(actor: &TrustedActor, required_scope: &'static str) -> Result<(), ApiError> {
    if actor.has_scope(required_scope) {
        return Ok(());
    }

    Err(ApiError::with_details(
        WurzburgResultCode::MissingRequiredScope,
        serde_json::json!({ "required_scope": required_scope }),
    ))
}

pub fn extract_trusted_actor(
    context: &TrustedRequestContext,
    config: &Wso2Config,
) -> Result<TrustedActor, ApiError> {
    let claims = verify_actor_jwt(
        context.backend_token.expose_for_signature_validation(),
        config,
    )?;
    TrustedActor::from_verified_claims(claims)
}

fn verify_actor_jwt(assertion: &str, config: &Wso2Config) -> Result<VerifiedActorClaims, ApiError> {
    let algorithms = parse_allowed_algorithms(&config.allowed_algorithms)?;
    let Some(first_algorithm) = algorithms.first().copied() else {
        return Err(ApiError::new(WurzburgResultCode::InvalidActorClaim));
    };

    let mut validation = Validation::new(first_algorithm);
    validation.algorithms = algorithms;
    validation.set_issuer(&[config.issuer.as_str()]);
    validation.set_audience(&[config.audience.as_str()]);
    validation.leeway = config.clock_skew_seconds;

    let decoding_key = DecodingKey::from_rsa_pem(config.public_key_pem.as_bytes())
        .map_err(|_| ApiError::new(WurzburgResultCode::InvalidActorClaim))?;
    let token = decode::<RawVerifiedActorClaims>(assertion, &decoding_key, &validation)
        .map_err(|_| ApiError::new(WurzburgResultCode::InvalidActorClaim))?;

    token.claims.into_verified_claims()
}

fn parse_allowed_algorithms(values: &[String]) -> Result<Vec<Algorithm>, ApiError> {
    values
        .iter()
        .map(|value| match value.as_str() {
            "RS256" => Ok(Algorithm::RS256),
            "RS384" => Ok(Algorithm::RS384),
            "RS512" => Ok(Algorithm::RS512),
            _ => Err(ApiError::new(WurzburgResultCode::InvalidActorClaim)),
        })
        .collect()
}

#[derive(Debug, Deserialize)]
struct RawVerifiedActorClaims {
    iss: String,
    aud: Value,
    sub: String,
    azp: Option<String>,
    client_id: Option<String>,
    exp: usize,
    nbf: Option<usize>,
    iat: Option<usize>,
    jti: Option<String>,
    #[serde(default)]
    roles: ClaimStrings,
    #[serde(default)]
    scope: ClaimStrings,
    #[serde(default)]
    scp: ClaimStrings,
    provider_id: Option<Uuid>,
    user_id: Option<Uuid>,
}

impl RawVerifiedActorClaims {
    fn into_verified_claims(self) -> Result<VerifiedActorClaims, ApiError> {
        let _registered_claims = (&self.aud, self.exp, self.nbf, self.iat, &self.jti);
        let client_id = match (self.azp, self.client_id) {
            (Some(azp), Some(client_id)) if azp != client_id => {
                return Err(ApiError::new(WurzburgResultCode::InvalidActorClaim));
            }
            (Some(azp), _) => azp,
            (_, Some(client_id)) => client_id,
            _ => return Err(ApiError::new(WurzburgResultCode::InvalidActorClaim)),
        };

        let mut scopes = self.scope.into_vec();
        scopes.extend(self.scp.into_vec());

        Ok(VerifiedActorClaims {
            issuer: self.iss,
            subject: self.sub,
            client_id,
            roles: self.roles.into_vec(),
            scopes,
            provider_id: self.provider_id,
            user_id: self.user_id,
        })
    }
}

#[derive(Debug, Default)]
struct ClaimStrings(Vec<String>);

impl ClaimStrings {
    fn into_vec(self) -> Vec<String> {
        self.0
    }
}

impl<'de> Deserialize<'de> for ClaimStrings {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = Option::<Value>::deserialize(deserializer)?;
        let Some(value) = value else {
            return Ok(Self::default());
        };

        match value {
            Value::String(value) => Ok(Self(
                value
                    .split_ascii_whitespace()
                    .map(ToOwned::to_owned)
                    .collect(),
            )),
            Value::Array(values) => values
                .into_iter()
                .map(|value| match value {
                    Value::String(value) => Ok(value),
                    _ => Err(serde::de::Error::custom(
                        "claim array values must be strings",
                    )),
                })
                .collect::<Result<Vec<_>, _>>()
                .map(Self),
            _ => Err(serde::de::Error::custom(
                "claim must be a string or string array",
            )),
        }
    }
}

/// WSO2 scopes are exact authorization strings, not prefixes or patterns.
/// Normalization trims accidental whitespace and rejects empty values so later
/// authorization checks can use deterministic set membership.
fn normalize_exact_claims(values: Vec<String>) -> Result<BTreeSet<String>, ApiError> {
    let mut normalized = BTreeSet::new();

    for value in values {
        let value = value.trim();
        if value.is_empty() || value.chars().any(char::is_control) {
            return Err(ApiError::new(WurzburgResultCode::InvalidActorClaim));
        }
        normalized.insert(value.to_string());
    }

    Ok(normalized)
}
