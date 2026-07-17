use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde::{Deserialize, Serialize};
use wurzburg::config::{BackendTokenTransport, Wso2Config};

const TEST_PRIVATE_KEY_PEM: &str = r#"-----BEGIN PRIVATE KEY-----
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

#[allow(dead_code)]
const TEST_PUBLIC_KEY_PEM: &str = r#"-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAm3uEcKrFiohsmsMvGIEm
MvMeGSdZ0Cz73s1bWxFm9O5t0NU5xWAsPq64lXqcQ3zUkLMkiyFEmMqDquTd8/L/
I5LCN5VjFblUSO2DREfIT0KAPdIUntFHreMgdrwOrfuP3b6IEwF56Gi5GHsaNSzJ
M6kuZqhd13bMd8O2RG9scviJ5GmTcpDQ3x23X08lQD+ozgAUsuKu9iBJtRfqaeI0
T62Jgv4/L64MoO3l5mMuYbxu5zSD9tM3Wnh2Q5ncYb4YyqHexpv/XetZrBtYHqjQ
QT5by9A9Zy4L+zd1B1AjdKJR7wVUm34+ngKETW95GK+sBX694tOmH7OrK1SA+M7k
8wIDAQAB
-----END PUBLIC KEY-----"#;

#[derive(Debug, Serialize, Deserialize)]
struct TestJwtClaims {
    iss: String,
    aud: String,
    sub: String,
    azp: Option<String>,
    client_id: Option<String>,
    exp: usize,
    nbf: usize,
    iat: usize,
    jti: String,
    roles: Vec<String>,
    scope: Vec<String>,
}

#[allow(dead_code)]
pub fn test_wso2_config() -> Wso2Config {
    Wso2Config {
        backend_token_transport: BackendTokenTransport::XJwtAssertion,
        accepted_correlation_pattern: "^[A-Za-z0-9._:-]{1,128}$".to_string(),
        issuer: "https://wso2.example.test".to_string(),
        audience: "wurzburg-api".to_string(),
        allowed_algorithms: vec!["RS256".to_string()],
        clock_skew_seconds: 60,
        public_key_pem: TEST_PUBLIC_KEY_PEM.to_string(),
    }
}

#[allow(dead_code)]
pub fn signed_platform_admin_jwt() -> String {
    signed_test_jwt(Some("portal"), None)
}

#[allow(dead_code)]
pub fn signed_conflicting_client_jwt() -> String {
    signed_test_jwt(Some("portal-a"), Some("portal-b"))
}

fn signed_test_jwt(azp: Option<&str>, client_id: Option<&str>) -> String {
    let claims = TestJwtClaims {
        iss: "https://wso2.example.test".to_string(),
        aud: "wurzburg-api".to_string(),
        sub: "admin-1".to_string(),
        azp: azp.map(ToOwned::to_owned),
        client_id: client_id.map(ToOwned::to_owned),
        exp: 2_000_000_000,
        nbf: 1_600_000_000,
        iat: 1_600_000_000,
        jti: uuid::Uuid::new_v4().to_string(),
        roles: vec!["wurzburg_platform_admin".to_string()],
        scope: vec![
            "platform.card_ranges:write".to_string(),
            "platform.card_ranges:read".to_string(),
        ],
    };

    encode(
        &Header::new(Algorithm::RS256),
        &claims,
        &EncodingKey::from_rsa_pem(TEST_PRIVATE_KEY_PEM.as_bytes()).expect("valid test RSA key"),
    )
    .expect("test JWT should sign")
}
