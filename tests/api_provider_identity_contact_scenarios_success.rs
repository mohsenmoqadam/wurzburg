use utoipa::OpenApi;
use wurzburg::api::swagger::ApiDoc;

/// Scenario goal: prove every provider identity/contact route is published with
/// the trusted WSO2 and idempotency headers required for manual and automated
/// production-equivalent testing.
#[test]
fn exposes_provider_identity_and_contact_contract_with_trusted_headers() {
    let document = serde_json::to_value(ApiDoc::openapi()).unwrap();
    for (path, method, mutating) in [
        ("/api/v1/providers/{provider_id}", "patch", true),
        ("/api/v1/providers/{provider_id}/contacts", "post", true),
        ("/api/v1/providers/{provider_id}/contacts", "get", false),
        (
            "/api/v1/providers/{provider_id}/contacts/{contact_id}",
            "patch",
            true,
        ),
        (
            "/api/v1/providers/{provider_id}/contacts/{contact_id}/suspend",
            "post",
            true,
        ),
        (
            "/api/v1/providers/{provider_id}/contacts/{contact_id}/reactivate",
            "post",
            true,
        ),
    ] {
        let operation = &document["paths"][path][method];
        assert!(operation.is_object(), "missing {method} {path}");
        let names: Vec<_> = operation["parameters"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|parameter| parameter["name"].as_str())
            .collect();
        for required in [
            "X-Correlation-Id",
            "X-Request-Id",
            "X-WSO2-Client-IP",
            "X-WSO2-Gateway-Id",
        ] {
            assert!(names.contains(&required), "{path} is missing {required}");
        }
        assert_eq!(names.contains(&"Idempotency-Key"), mutating);
        assert!(operation["security"].is_array());
    }
}
