CREATE TABLE providers (
    provider_id RAW(16) PRIMARY KEY,
    legal_name VARCHAR2(255) NOT NULL,
    trade_name VARCHAR2(255) NOT NULL,
    tax_id VARCHAR2(64),
    registration_number VARCHAR2(64),
    email_address VARCHAR2(255),
    website_url VARCHAR2(512),
    mailing_address VARCHAR2(1000),
    status VARCHAR2(32) NOT NULL,
    metadata_json JSON DEFAULT '{}' NOT NULL,
    created_by_subject VARCHAR2(255) NOT NULL,
    updated_by_subject VARCHAR2(255) NOT NULL,
    created_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    updated_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT ck_providers_status CHECK (
        status IN ('DRAFT', 'PENDING_PROVISIONING', 'READY', 'ACTIVE', 'SUSPENDED', 'INACTIVE', 'FAILED')
    )
);

CREATE TABLE card_ranges (
    card_range_id RAW(16) PRIMARY KEY,
    start_card_number VARCHAR2(16) NOT NULL,
    end_card_number VARCHAR2(16) NOT NULL,
    funding_mode VARCHAR2(32) NOT NULL,
    withdrawal_limit_authority VARCHAR2(16) NOT NULL,
    limit_calendar_json JSON,
    status VARCHAR2(32) DEFAULT 'DRAFT' NOT NULL,
    issuance_enabled NUMBER(1) DEFAULT 1 NOT NULL,
    cms_operation_mode VARCHAR2(32) DEFAULT 'FULL' NOT NULL,
    operational_version NUMBER(19,0) DEFAULT 1 NOT NULL,
    metadata_json JSON DEFAULT '{}' NOT NULL,
    created_by_subject VARCHAR2(255) NOT NULL,
    updated_by_subject VARCHAR2(255) NOT NULL,
    created_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    updated_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT ck_card_ranges_funding_mode CHECK (
        funding_mode IN ('SINGLE_PROVIDER', 'MULTI_PROVIDER')
    ),
    CONSTRAINT ck_card_ranges_authority CHECK (
        withdrawal_limit_authority IN ('PLATFORM', 'CMS')
    ),
    CONSTRAINT ck_card_ranges_status CHECK (
        status IN ('DRAFT', 'ACTIVE', 'SUSPENDED')
    ),
    CONSTRAINT ck_card_ranges_cms_mode CHECK (
        cms_operation_mode IN ('FULL', 'BALANCE_ONLY', 'BLOCKED')
    ),
    CONSTRAINT ck_card_ranges_order CHECK (
        start_card_number <= end_card_number
    ),
    CONSTRAINT ck_card_ranges_issuance CHECK (
        issuance_enabled IN (0, 1)
    ),
    CONSTRAINT ck_card_ranges_authority_calendar CHECK (
        (withdrawal_limit_authority = 'PLATFORM' AND limit_calendar_json IS JSON)
        OR (withdrawal_limit_authority = 'CMS' AND limit_calendar_json IS NULL)
    )
);

CREATE INDEX idx_card_ranges_lookup
    ON card_ranges(status, start_card_number, end_card_number);

CREATE INDEX idx_card_ranges_mode_status
    ON card_ranges(funding_mode, status);

CREATE INDEX idx_card_ranges_authority_status
    ON card_ranges(withdrawal_limit_authority, status);

CREATE TABLE card_range_providers (
    card_range_id RAW(16) NOT NULL,
    provider_id RAW(16) NOT NULL,
    status VARCHAR2(32) NOT NULL,
    metadata_json JSON DEFAULT '{}' NOT NULL,
    created_by_subject VARCHAR2(255) NOT NULL,
    updated_by_subject VARCHAR2(255) NOT NULL,
    created_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    updated_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT pk_card_range_providers
        PRIMARY KEY (card_range_id, provider_id),
    CONSTRAINT fk_crp_card_range
        FOREIGN KEY (card_range_id) REFERENCES card_ranges(card_range_id),
    CONSTRAINT fk_crp_provider
        FOREIGN KEY (provider_id) REFERENCES providers(provider_id),
    CONSTRAINT ck_crp_status CHECK (
        status IN ('ACTIVE', 'SUSPENDED')
    )
);

CREATE INDEX idx_crp_range_status
    ON card_range_providers(card_range_id, status);

CREATE INDEX idx_crp_provider_status
    ON card_range_providers(provider_id, status);

CREATE UNIQUE INDEX uq_crp_one_active_range_per_provider
    ON card_range_providers (
        CASE WHEN status = 'ACTIVE' THEN provider_id END
    );

CREATE TABLE card_policy_profiles (
    card_policy_profile_id RAW(16) PRIMARY KEY,
    card_range_id RAW(16) NOT NULL,
    profile_json JSON NOT NULL,
    status VARCHAR2(32) NOT NULL,
    version NUMBER(19,0) NOT NULL,
    effective_at TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    superseded_by_profile_id RAW(16),
    publication_operation_id RAW(16) NOT NULL,
    created_by_subject VARCHAR2(255) NOT NULL,
    change_reason VARCHAR2(1000) NOT NULL,
    activated_at TIMESTAMP(6) WITH TIME ZONE,
    superseded_at TIMESTAMP(6) WITH TIME ZONE,
    cancelled_by_subject VARCHAR2(255),
    cancelled_at TIMESTAMP(6) WITH TIME ZONE,
    cancel_reason VARCHAR2(1000),
    created_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    status_updated_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT fk_cpp_card_range
        FOREIGN KEY (card_range_id) REFERENCES card_ranges(card_range_id),
    CONSTRAINT fk_cpp_superseded_by
        FOREIGN KEY (superseded_by_profile_id)
        REFERENCES card_policy_profiles(card_policy_profile_id),
    CONSTRAINT ck_cpp_status CHECK (
        status IN ('SCHEDULED', 'PUBLISHING', 'ACTIVE', 'SUPERSEDED', 'CANCELLED', 'PUBLICATION_FAILED')
    ),
    CONSTRAINT uq_cpp_range_version UNIQUE (card_range_id, version)
);

CREATE UNIQUE INDEX uq_cpp_one_active
    ON card_policy_profiles (
        CASE WHEN status = 'ACTIVE' THEN card_range_id END
    );

CREATE UNIQUE INDEX uq_cpp_one_candidate
    ON card_policy_profiles (
        CASE WHEN status IN ('SCHEDULED', 'PUBLISHING', 'PUBLICATION_FAILED') THEN card_range_id END
    );

CREATE INDEX idx_cpp_status_effective
    ON card_policy_profiles(status, effective_at);
