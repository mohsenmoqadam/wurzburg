use wurzburg::api::auth::{TrustedActor, VerifiedActorClaims, require_scope};

#[test]
fn rejects_missing_required_scope() {
    let actor = TrustedActor::from_verified_claims(VerifiedActorClaims {
        issuer: "https://wso2.example.test".to_string(),
        subject: "admin@example.test".to_string(),
        client_id: "admin-ui".to_string(),
        roles: vec!["wurzburg_platform_admin".to_string()],
        scopes: vec!["platform.card_ranges:read".to_string()],
        provider_id: None,
        user_id: None,
    })
    .expect("verified actor claims should be accepted");

    let error =
        require_scope(&actor, "platform.card_ranges:write").expect_err("missing scope should fail");

    assert_eq!(error.body().error.code, "MISSING_REQUIRED_SCOPE");
}

#[test]
fn rejects_prefix_scope_match() {
    let actor = TrustedActor::from_verified_claims(VerifiedActorClaims {
        issuer: "https://wso2.example.test".to_string(),
        subject: "admin@example.test".to_string(),
        client_id: "admin-ui".to_string(),
        roles: vec!["wurzburg_platform_admin".to_string()],
        scopes: vec!["platform.card_ranges".to_string()],
        provider_id: None,
        user_id: None,
    })
    .expect("verified actor claims should be accepted");

    let error = require_scope(&actor, "platform.card_ranges:write")
        .expect_err("prefix matching must not authorize");

    assert_eq!(error.body().error.code, "MISSING_REQUIRED_SCOPE");
}

#[test]
fn rejects_empty_actor_subject() {
    let error = TrustedActor::from_verified_claims(VerifiedActorClaims {
        issuer: "https://wso2.example.test".to_string(),
        subject: " ".to_string(),
        client_id: "admin-ui".to_string(),
        roles: vec![],
        scopes: vec!["platform.card_ranges:write".to_string()],
        provider_id: None,
        user_id: None,
    })
    .expect_err("empty subject should fail");

    assert_eq!(error.body().error.code, "INVALID_ACTOR_CLAIM");
}
