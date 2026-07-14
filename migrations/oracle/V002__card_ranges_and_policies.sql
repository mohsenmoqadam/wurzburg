-- Card range and range-scoped card policy foundation.

CREATE TABLE card_ranges (
    card_range_id RAW(16) PRIMARY KEY,
    start_card_number VARCHAR2(32) NOT NULL,
    end_card_number VARCHAR2(32) NOT NULL,
    funding_mode VARCHAR2(32) NOT NULL,
    status VARCHAR2(32) NOT NULL,
    metadata_json JSON,
    created_by VARCHAR2(255) NOT NULL,
    updated_by VARCHAR2(255) NOT NULL,
    created_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    updated_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT ck_card_ranges_funding_mode CHECK (
        funding_mode IN ('SINGLE_PROVIDER', 'MULTI_PROVIDER')
    ),
    CONSTRAINT ck_card_ranges_status CHECK (
        status IN ('DRAFT', 'ACTIVE', 'SUSPENDED')
    ),
    CONSTRAINT ck_card_ranges_order CHECK (
        start_card_number <= end_card_number
    )
);

CREATE INDEX idx_card_ranges_lookup
    ON card_ranges(status, start_card_number, end_card_number);

CREATE INDEX idx_card_ranges_mode_status
    ON card_ranges(funding_mode, status);

CREATE TABLE card_range_providers (
    card_range_id RAW(16) NOT NULL,
    provider_id RAW(16) NOT NULL,
    status VARCHAR2(32) NOT NULL,
    metadata_json JSON,
    created_by VARCHAR2(255) NOT NULL,
    updated_by VARCHAR2(255) NOT NULL,
    created_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    updated_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT pk_card_range_providers
        PRIMARY KEY (card_range_id, provider_id),
    CONSTRAINT fk_crp_card_range
        FOREIGN KEY (card_range_id) REFERENCES card_ranges(card_range_id),
    CONSTRAINT fk_crp_provider
        FOREIGN KEY (provider_id) REFERENCES providers(id),
    CONSTRAINT ck_crp_status CHECK (
        status IN ('ACTIVE', 'SUSPENDED')
    )
);

CREATE INDEX idx_crp_range_status
    ON card_range_providers(card_range_id, status);

CREATE INDEX idx_crp_provider_status
    ON card_range_providers(provider_id, status);

CREATE TABLE card_policy_profiles (
    card_policy_profile_id RAW(16) PRIMARY KEY,
    profile_json JSON NOT NULL,
    status VARCHAR2(32) NOT NULL,
    version NUMBER(19,0) NOT NULL,
    effective_at TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    superseded_by_profile_id RAW(16),
    created_by VARCHAR2(255) NOT NULL,
    created_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT fk_cpp_superseded_by
        FOREIGN KEY (superseded_by_profile_id)
        REFERENCES card_policy_profiles(card_policy_profile_id),
    CONSTRAINT ck_cpp_status CHECK (
        status IN ('DRAFT', 'ACTIVE', 'SUPERSEDED', 'SUSPENDED')
    )
);

CREATE TABLE card_range_policy_assignments (
    card_range_id RAW(16) NOT NULL,
    card_policy_profile_id RAW(16) NOT NULL,
    status VARCHAR2(32) NOT NULL,
    assigned_by VARCHAR2(255) NOT NULL,
    assigned_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    superseded_at TIMESTAMP(6) WITH TIME ZONE,
    CONSTRAINT pk_card_range_policy_assignments
        PRIMARY KEY (card_range_id, card_policy_profile_id),
    CONSTRAINT fk_crpa_card_range
        FOREIGN KEY (card_range_id) REFERENCES card_ranges(card_range_id),
    CONSTRAINT fk_crpa_policy_profile
        FOREIGN KEY (card_policy_profile_id)
        REFERENCES card_policy_profiles(card_policy_profile_id),
    CONSTRAINT ck_crpa_status CHECK (
        status IN ('ACTIVE', 'SUPERSEDED')
    )
);

CREATE UNIQUE INDEX uq_crpa_one_active
    ON card_range_policy_assignments (
        CASE WHEN status = 'ACTIVE' THEN card_range_id END
    );

CREATE INDEX idx_crpa_range_status
    ON card_range_policy_assignments(card_range_id, status);
