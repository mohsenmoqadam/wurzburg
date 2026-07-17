use uuid::Uuid;
use wurzburg::api::auth::{TrustedActor, VerifiedActorClaims, require_scope};

fn actor_with_scopes(scopes: Vec<&str>) -> TrustedActor {
    TrustedActor::from_verified_claims(VerifiedActorClaims {
        issuer: "https://wso2.example.test".to_string(),
        subject: "admin@example.test".to_string(),
        client_id: "admin-ui".to_string(),
        roles: vec!["wurzburg_platform_admin".to_string()],
        scopes: scopes.into_iter().map(ToOwned::to_owned).collect(),
        provider_id: Some(Uuid::new_v4()),
        user_id: None,
    })
    .expect("verified actor claims should be accepted")
}

#[test]
fn accepts_required_exact_scope() {
    let actor = actor_with_scopes(vec!["platform.card_ranges:write"]);

    require_scope(&actor, "platform.card_ranges:write").expect("scope should be accepted");
}

#[test]
fn trims_and_deduplicates_verified_actor_scopes() {
    let actor = actor_with_scopes(vec![
        " platform.card_ranges:write ",
        "platform.card_ranges:write",
    ]);

    assert_eq!(
        actor.scopes().collect::<Vec<_>>(),
        vec!["platform.card_ranges:write"]
    );
}
