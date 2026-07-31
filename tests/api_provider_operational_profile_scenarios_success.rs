use wurzburg::api::swagger::ApiDoc;

use utoipa::OpenApi;

/// Contract proof: operational-profile routes are visible to WSO2 consumers,
/// and every mutating route advertises the trusted and idempotency headers used
/// by the production HTTP boundary.
#[test]
fn exposes_provider_operational_profile_contract_with_trusted_headers() {
    let document = serde_json::to_value(ApiDoc::openapi()).unwrap();
    let paths = document["paths"].as_object().unwrap();
    for path in [
        "/api/v1/providers/{provider_id}/operational-profile",
        "/api/v1/providers/{provider_id}/operational-profiles",
        "/api/v1/providers/{provider_id}/operational-profiles/{profile_id}/cancel",
    ] {
        assert!(paths.contains_key(path), "missing OpenAPI path {path}");
    }

    for operation in [
        &paths["/api/v1/providers/{provider_id}/operational-profiles"]["post"],
        &paths["/api/v1/providers/{provider_id}/operational-profiles/{profile_id}/cancel"]["post"],
    ] {
        let parameters = operation["parameters"].as_array().unwrap();
        for required in [
            "Idempotency-Key",
            "X-Correlation-Id",
            "X-Request-Id",
            "X-WSO2-Client-IP",
            "X-WSO2-Gateway-Id",
        ] {
            assert!(
                parameters
                    .iter()
                    .any(|parameter| parameter["name"] == required),
                "missing OpenAPI header {required}"
            );
        }
    }
}
