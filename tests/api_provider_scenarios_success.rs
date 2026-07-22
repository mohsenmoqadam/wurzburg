use utoipa::OpenApi;
use wurzburg::api::swagger::ApiDoc;

#[test]
fn swagger_exposes_provider_core_routes_and_idempotency_contract() {
    let document = serde_json::to_value(ApiDoc::openapi()).expect("OpenAPI should serialize");
    let paths = document["paths"].as_object().expect("paths should exist");
    assert!(paths.contains_key("/api/v1/providers"));
    assert!(paths.contains_key("/api/v1/providers/{provider_id}"));
    let parameters = paths["/api/v1/providers"]["post"]["parameters"]
        .as_array()
        .expect("create parameters should exist");
    assert!(
        parameters
            .iter()
            .any(|parameter| parameter["name"] == "Idempotency-Key")
    );
}
