//! Development-only WSO2 backend JWT generator for local Swagger/API testing.
//!
//! This binary intentionally emits a broad, long-lived token for developer
//! convenience. It is not a production authentication path and must never be
//! used by deployed Wurzburg services, CI secrets, WSO2 configuration, or
//! customer-facing environments.
//!
//! Maintenance rule:
//! - Keep `FULL_ACCESS_SCOPES` aligned with `WURZBURG_WSO2_HANDOFF.md`.
//! - Add new scopes here when new Wurzburg APIs are introduced.
//! - Keep stdout as the token only so developers can pipe the command directly.
//!
//! Usage:
//! `cargo run --bin get-wurzburg-dev-jwt`

use chrono::Utc;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde::Serialize;
use uuid::Uuid;

const ISSUER: &str = "https://wso2.example.test";
const AUDIENCE: &str = "wurzburg-api";
const SUBJECT: &str = "admin-1";
const CLIENT_ID: &str = "wurzburg-dev-swagger";
const TTL_DAYS: i64 = 365;

const FULL_ACCESS_ROLES: &[&str] = &[
    "wurzburg_platform_admin",
    "wurzburg_provider_admin",
    "wurzburg_cardholder",
    "wurzburg_support",
    "wurzburg_reporter",
];

const FULL_ACCESS_SCOPES: &[&str] = &[
    "provider.users:read",
    "provider.users:write",
    "provider.cards:read",
    "provider.cards:write",
    "provider.funding:read",
    "provider.funding:write",
    "provider.transactions:read",
    "provider.kafka_credentials:read",
    "provider.kafka_credentials:rotate",
    "provider.events:read",
    "card.funding-order:read",
    "card.funding-order:write",
    "card.credit:read",
    "card.credit:return",
    "card.transactions:read",
    "platform.providers:read",
    "platform.providers:write",
    "platform.audit:read",
    "platform.card_ranges:read",
    "platform.card_ranges:write",
    "platform.card_issuance:read",
    "platform.card_issuance:write",
    "platform.policies:read",
    "platform.policies:write",
    "platform.fee_profiles:read",
    "platform.fee_profiles:write",
    "platform.provider_events:read",
    "platform.provider_events:write",
    "platform.provider_events:replay",
    "platform.config:read",
    "platform.config:write",
    "platform.recovery:read",
    "platform.recovery:write",
    "support.users:read",
    "support.cards:read",
    "support.transactions:read",
    "support.recovery:read",
    "reports.transactions:read",
    "reports.transactions:export",
];

const DEV_PRIVATE_KEY_PEM: &str = r#"-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCbe4RwqsWKiGya
wy8YgSYy8x4ZJ1nQLPvezVtbEWb07m3Q1TnFYCw+rriVepxDfNSQsySLIUSYyoOq
5N3z8v8jksI3lWMVuVRI7YNER8hPQoA90hSe0Uet4yB2vA6t+4/dvogTAXnoaLkY
exo1LMkzqS5mqF3Xdsx3w7ZEb2xy+InkaZNykNDfHbdfTyVAP6jOABSy4q72IEm1
F+pp4jRPrYmC/j8vrgyg7eXmYy5hvG7nNIP20zdaeHZDmdxhvhjKod7Gm/9d61ms
G1geqNBBPlvL0D1nLgv7N3UHUCN0olHvBVSbfj6eAoRNb3kYr6wFfr3i06Yfs6sr
VID4zuTzAgMBAAECggEADEvdcoee7dDSPf8Xt2lnWvotNDIPgU49cSZuhio/KTm+
B5kFY52yghaRVIkI2LGDohn41uP/p9HETfyhrQxXrzmJEJpI5svsZYQbMIg4yEPr
HniB1vmYIKFozNsckhfmDdRmsJIaUQ4PLWd28COlmedUWxwPp92mWAZrYMgrHIB5
16kh6j3Q7Q9K8rF9RrjGQTpveJR+/cyd91qdW9bMGBOqs+xdV/bduIQZAKSU0vsm
9RhDzyfhjPNG/edK0JsR6AClHlPy6QWOGWuYdvlNhzOCN4N4KXmNWSb+/45eIzGI
vLQUQEhcl6TGRMEgpxUHXPItSR5uVW+NwV2dDgR9EQKBgQDRNz93YNLyxJpwVLcO
fTjMifeekmZh75ZXNzGlgKqiCvc5u2Ab+050MGI9qTTWHvZwVPYGgxeZcHSLSXwo
aaw6uSsaiP5J20B9Nk71wIt7gerui7P2uBd2721bg8woIbBlF6nPLbaHCz5mQSzF
9m133pvdIpMKDVyRdrcc4brEmQKBgQC+QEF+PhvzmGcfoIcALdwWf8N5rq/8gGir
HxZK6hkyzKbDk07Zi2Rm7gLEAp9Uxo2XbZWGH9SsWFESSMip2UE1lnM0UCKDVFCE
S9R9gVe8vhbZ6YqVw4BW+X9P91KBfrkHWmd587Oe4H3JgvrdAhIS3N63sQERobtC
KWjEeP8hawKBgQCQ60gXFQaKCw0/Si8S5kJ1zAut15L7u83T0/ObxKhtXlMptlU4
jLcnXGxwccibmQ7zeKaClEPAkVjpMpnCFJCsjJ8C3mnmFu1wzjGboSf9AV0Op86c
05/NTsPdZEoCcnORUvbY/70zheJPSk4NQklJgvVMFCruB5tbV3Q3mVSZ4QKBgHlh
m6eEzuaTBLBcBeXqXHIKX9gByQxrjNwowFtZkmwjv/4lvPf1BEDbd+5A0hEPgQTt
CKoDIvg2fLsSrtwW3ZDoBWaJ/gsWPyy5CMBuRmEIUqIDa8Tzb62OD1kgrYYrKLf1
SPG4t5AVIIvxwkZBbPCV9I70In9yVXv32X0IyZYzAoGBAIpDGw31ULAMPA/01mOk
24HrkRRZct8UWkt+cTNw2gfToTd+kkSmSj2xXSzzusO14vPaLmlz4XwkPzxt3VHg
GkIrxMiTopDNmZef+iajz1AXvyx7BYBzOc1AVRB0226MPliKR3bhLZmL+OepmIW2
qFYobwYsqUjwG6VHfWkjRBP5
-----END PRIVATE KEY-----"#;

#[derive(Debug, Serialize)]
struct DevJwtClaims {
    iss: String,
    aud: String,
    sub: String,
    azp: String,
    client_id: String,
    exp: usize,
    nbf: usize,
    iat: usize,
    jti: String,
    roles: Vec<String>,
    scope: Vec<String>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let now = Utc::now().timestamp();
    let expires_at = now + TTL_DAYS * 24 * 60 * 60;
    let claims = DevJwtClaims {
        iss: ISSUER.to_string(),
        aud: AUDIENCE.to_string(),
        sub: SUBJECT.to_string(),
        azp: CLIENT_ID.to_string(),
        client_id: CLIENT_ID.to_string(),
        exp: expires_at as usize,
        nbf: now as usize,
        iat: now as usize,
        jti: Uuid::new_v4().to_string(),
        roles: FULL_ACCESS_ROLES
            .iter()
            .map(|role| (*role).to_string())
            .collect(),
        scope: FULL_ACCESS_SCOPES
            .iter()
            .map(|scope| (*scope).to_string())
            .collect(),
    };

    let token = encode(
        &Header::new(Algorithm::RS256),
        &claims,
        &EncodingKey::from_rsa_pem(DEV_PRIVATE_KEY_PEM.as_bytes())?,
    )?;

    println!("{token}");
    Ok(())
}
