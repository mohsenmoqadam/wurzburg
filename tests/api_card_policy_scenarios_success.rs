use utoipa::OpenApi;
use wurzburg::api::swagger::ApiDoc;

#[test]
fn swagger_exposes_final_card_policy_routes_and_trusted_headers() {
    let document = ApiDoc::openapi();
    let paths = document.paths.paths;

    let current = paths
        .get("/api/v1/card-ranges/{card_range_id}/policy")
        .expect("current policy path should exist");
    assert!(current.get.is_some());
    let put = current.put.as_ref().expect("policy PUT should exist");
    let parameter_names: Vec<_> = put
        .parameters
        .as_ref()
        .expect("policy PUT parameters should exist")
        .iter()
        .map(|parameter| parameter.name.as_str())
        .collect();
    for required in [
        "card_range_id",
        "Idempotency-Key",
        "X-Correlation-Id",
        "X-Request-Id",
        "X-WSO2-Client-IP",
        "X-WSO2-Gateway-Id",
    ] {
        assert!(parameter_names.contains(&required), "missing {required}");
    }

    let history = paths
        .get("/api/v1/card-ranges/{card_range_id}/policies")
        .expect("policy history path should exist");
    assert!(history.get.is_some());
    let by_id = paths
        .get("/api/v1/card-ranges/{card_range_id}/policies/{policy_id}")
        .expect("policy by-ID path should exist");
    assert!(by_id.get.is_some());
}
