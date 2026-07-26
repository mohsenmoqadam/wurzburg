use utoipa::OpenApi;
use wurzburg::api::swagger::ApiDoc;

#[test]
fn swagger_exposes_provider_fee_routes_and_trusted_headers() {
    let paths = ApiDoc::openapi().paths.paths;
    for path in [
        "/api/v1/providers/{provider_id}/fee-profile",
        "/api/v1/providers/{provider_id}/fee-profiles",
        "/api/v1/providers/{provider_id}/fee-profiles/{fee_profile_id}",
    ] {
        assert!(paths.contains_key(path), "missing provider fee path {path}");
    }

    let operation = paths["/api/v1/providers/{provider_id}/fee-profile"]
        .put
        .as_ref()
        .expect("fee profile PUT should exist");
    let names: Vec<_> = operation
        .parameters
        .as_ref()
        .expect("fee profile parameters should exist")
        .iter()
        .map(|parameter| parameter.name.as_str())
        .collect();
    for required in [
        "provider_id",
        "Idempotency-Key",
        "X-Correlation-Id",
        "X-Request-Id",
        "X-WSO2-Client-IP",
        "X-WSO2-Gateway-Id",
    ] {
        assert!(names.contains(&required), "missing {required}");
    }
}
