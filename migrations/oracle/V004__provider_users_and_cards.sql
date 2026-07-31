CREATE TABLE user_identity_allocation_locks (
    national_id_hash VARCHAR2(64) PRIMARY KEY,
    created_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
);

CREATE TABLE users (
    user_id RAW(16) PRIMARY KEY,
    national_id VARCHAR2(10) NOT NULL UNIQUE,
    first_name VARCHAR2(255) NOT NULL,
    last_name VARCHAR2(255) NOT NULL,
    birth_date DATE,
    mobile VARCHAR2(64),
    metadata_json JSON DEFAULT '{}' NOT NULL,
    status VARCHAR2(32) DEFAULT 'ACTIVE' NOT NULL,
    created_by_subject VARCHAR2(255) NOT NULL,
    updated_by_subject VARCHAR2(255) NOT NULL,
    created_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    updated_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT ck_users_status CHECK (status IN ('ACTIVE', 'SUSPENDED'))
);

CREATE TABLE provider_users (
    provider_user_id RAW(16) PRIMARY KEY,
    enrollment_id RAW(16) NOT NULL UNIQUE,
    provider_id RAW(16) NOT NULL,
    user_id RAW(16) NOT NULL,
    provider_customer_reference VARCHAR2(255) NOT NULL,
    supplied_first_name VARCHAR2(255) NOT NULL,
    supplied_last_name VARCHAR2(255) NOT NULL,
    identity_mismatch NUMBER(1) DEFAULT 0 NOT NULL,
    mismatch_fields_json JSON DEFAULT '[]' NOT NULL,
    selection_reference VARCHAR2(255) NOT NULL,
    status VARCHAR2(32) NOT NULL,
    metadata_json JSON DEFAULT '{}' NOT NULL,
    created_by_subject VARCHAR2(255) NOT NULL,
    updated_by_subject VARCHAR2(255) NOT NULL,
    created_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    updated_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT fk_provider_users_provider FOREIGN KEY (provider_id) REFERENCES providers(provider_id),
    CONSTRAINT fk_provider_users_user FOREIGN KEY (user_id) REFERENCES users(user_id),
    CONSTRAINT uq_provider_users_relationship UNIQUE (provider_id, user_id),
    CONSTRAINT uq_provider_users_reference UNIQUE (provider_id, provider_customer_reference),
    CONSTRAINT ck_provider_users_mismatch CHECK (identity_mismatch IN (0, 1)),
    CONSTRAINT ck_provider_users_status CHECK (
        status IN ('CARD_ISSUANCE_PENDING', 'PROVISIONING', 'ACTIVE', 'SUSPENDED', 'ISSUANCE_REJECTED', 'RECOVERY_REQUIRED')
    )
);

CREATE INDEX idx_provider_users_list
    ON provider_users(provider_id, status, created_at, provider_user_id);

CREATE TABLE provider_user_accounts (
    provider_user_account_id RAW(16) PRIMARY KEY,
    provider_user_id RAW(16) NOT NULL UNIQUE,
    provider_id RAW(16) NOT NULL,
    user_id RAW(16) NOT NULL,
    tigerbeetle_account_id RAW(16) NOT NULL UNIQUE,
    status VARCHAR2(32) NOT NULL,
    created_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    updated_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT fk_pua_provider_user FOREIGN KEY (provider_user_id) REFERENCES provider_users(provider_user_id),
    CONSTRAINT fk_pua_provider FOREIGN KEY (provider_id) REFERENCES providers(provider_id),
    CONSTRAINT fk_pua_user FOREIGN KEY (user_id) REFERENCES users(user_id),
    CONSTRAINT uq_pua_provider_user UNIQUE (provider_id, user_id),
    CONSTRAINT ck_pua_status CHECK (status IN ('PROVISIONING', 'ACTIVE', 'SUSPENDED', 'CLOSED', 'RECOVERY_REQUIRED'))
);

CREATE TABLE cards (
    card_id RAW(16) PRIMARY KEY,
    card_number VARCHAR2(16) NOT NULL UNIQUE,
    user_id RAW(16) NOT NULL,
    card_range_id RAW(16) NOT NULL,
    status VARCHAR2(32) NOT NULL,
    metadata_json JSON DEFAULT '{}' NOT NULL,
    state_version NUMBER(19,0) DEFAULT 1 NOT NULL,
    publication_operation_id RAW(16),
    materialized_version NUMBER(19,0) DEFAULT 0 NOT NULL,
    materialized_at TIMESTAMP(6) WITH TIME ZONE,
    created_by_subject VARCHAR2(255) NOT NULL,
    updated_by_subject VARCHAR2(255) NOT NULL,
    created_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    updated_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT fk_cards_user FOREIGN KEY (user_id) REFERENCES users(user_id),
    CONSTRAINT fk_cards_range FOREIGN KEY (card_range_id) REFERENCES card_ranges(card_range_id),
    CONSTRAINT ck_cards_status CHECK (
        status IN ('PROVISIONING', 'ACTIVE', 'SUSPENDED', 'REPLACED', 'EXPIRED', 'RECOVERY_REQUIRED')
    ),
    CONSTRAINT ck_cards_versions CHECK (
        state_version >= 1 AND materialized_version >= 0 AND materialized_version <= state_version
    )
);

CREATE UNIQUE INDEX uq_cards_one_active_per_user_range
    ON cards (
        CASE WHEN status = 'ACTIVE' THEN user_id END,
        CASE WHEN status = 'ACTIVE' THEN card_range_id END
    );

CREATE INDEX idx_cards_provider_queries
    ON cards(user_id, card_range_id, status, created_at, card_id);

CREATE TABLE card_policy_usage_accounts (
    card_id RAW(16) PRIMARY KEY,
    amount_daily_account_id RAW(16) NOT NULL UNIQUE,
    amount_weekly_account_id RAW(16) NOT NULL UNIQUE,
    amount_monthly_account_id RAW(16) NOT NULL UNIQUE,
    amount_yearly_account_id RAW(16) NOT NULL UNIQUE,
    count_daily_account_id RAW(16) NOT NULL UNIQUE,
    count_weekly_account_id RAW(16) NOT NULL UNIQUE,
    count_monthly_account_id RAW(16) NOT NULL UNIQUE,
    count_yearly_account_id RAW(16) NOT NULL UNIQUE,
    status VARCHAR2(32) NOT NULL,
    created_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    updated_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT fk_cpua_card FOREIGN KEY (card_id) REFERENCES cards(card_id),
    CONSTRAINT ck_cpua_status CHECK (status IN ('PROVISIONING', 'ACTIVE', 'CLOSED', 'RECOVERY_REQUIRED'))
);

CREATE TABLE card_provider_funding_sources (
    card_funding_source_id RAW(16) PRIMARY KEY,
    card_id RAW(16) NOT NULL,
    provider_id RAW(16) NOT NULL,
    provider_user_id RAW(16) NOT NULL,
    provider_user_account_id RAW(16) NOT NULL,
    priority NUMBER(5,0),
    max_amount_rials NUMBER(19,0),
    status VARCHAR2(32) NOT NULL,
    created_by_subject VARCHAR2(255) NOT NULL,
    updated_by_subject VARCHAR2(255) NOT NULL,
    created_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    updated_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT fk_cpfs_card FOREIGN KEY (card_id) REFERENCES cards(card_id),
    CONSTRAINT fk_cpfs_provider FOREIGN KEY (provider_id) REFERENCES providers(provider_id),
    CONSTRAINT fk_cpfs_provider_user FOREIGN KEY (provider_user_id) REFERENCES provider_users(provider_user_id),
    CONSTRAINT fk_cpfs_account FOREIGN KEY (provider_user_account_id) REFERENCES provider_user_accounts(provider_user_account_id),
    CONSTRAINT uq_cpfs_card_provider UNIQUE (card_id, provider_id),
    CONSTRAINT ck_cpfs_priority CHECK (priority IS NULL OR priority BETWEEN 1 AND 65535),
    CONSTRAINT ck_cpfs_max_amount CHECK (max_amount_rials IS NULL OR max_amount_rials BETWEEN 0 AND 9007199254740991),
    CONSTRAINT ck_cpfs_status CHECK (status IN ('PROVISIONING', 'ACTIVE', 'SUSPENDED', 'CLOSED', 'RECOVERY_REQUIRED')),
    CONSTRAINT ck_cpfs_active_shape CHECK (status <> 'ACTIVE' OR priority IS NOT NULL)
);

CREATE UNIQUE INDEX uq_cpfs_active_priority
    ON card_provider_funding_sources (
        CASE WHEN status = 'ACTIVE' THEN card_id END,
        CASE WHEN status = 'ACTIVE' THEN priority END
    );

CREATE UNIQUE INDEX uq_cpfs_account_one_active_card
    ON card_provider_funding_sources (
        CASE WHEN status = 'ACTIVE' THEN provider_user_account_id END
    );

CREATE TABLE card_issuance_batches (
    card_issuance_batch_id RAW(16) PRIMARY KEY,
    status VARCHAR2(32) NOT NULL,
    request_object_key VARCHAR2(1000) NOT NULL,
    request_checksum_sha256 VARCHAR2(64),
    result_object_key VARCHAR2(1000),
    result_checksum_sha256 VARCHAR2(64),
    request_count NUMBER(10,0) DEFAULT 0 NOT NULL,
    issued_count NUMBER(10,0) DEFAULT 0 NOT NULL,
    rejected_count NUMBER(10,0) DEFAULT 0 NOT NULL,
    failed_count NUMBER(10,0) DEFAULT 0 NOT NULL,
    expires_at TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    created_by_subject VARCHAR2(255) NOT NULL,
    updated_by_subject VARCHAR2(255) NOT NULL,
    created_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    updated_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    completed_at TIMESTAMP(6) WITH TIME ZONE,
    CONSTRAINT ck_cib_status CHECK (
        status IN ('CREATING', 'READY', 'PROCESSING_RESULT', 'COMPLETED', 'PARTIALLY_COMPLETED', 'FAILED')
    ),
    CONSTRAINT ck_cib_counts CHECK (
        request_count >= 0 AND issued_count >= 0 AND rejected_count >= 0 AND failed_count >= 0
    )
);

CREATE INDEX idx_cib_list
    ON card_issuance_batches(status, created_at, card_issuance_batch_id);

CREATE TABLE card_issuance_requests (
    card_issuance_request_id RAW(16) PRIMARY KEY,
    user_id RAW(16) NOT NULL,
    card_range_id RAW(16) NOT NULL,
    status VARCHAR2(32) NOT NULL,
    identity_snapshot_json JSON NOT NULL,
    delivery_snapshot_json JSON NOT NULL,
    batch_id RAW(16),
    issuer_reference VARCHAR2(255),
    failure_code VARCHAR2(128),
    safe_failure_message VARCHAR2(1000),
    produced_at TIMESTAMP(6) WITH TIME ZONE,
    dispatched_at TIMESTAMP(6) WITH TIME ZONE,
    tracking_reference VARCHAR2(255),
    created_by_subject VARCHAR2(255) NOT NULL,
    updated_by_subject VARCHAR2(255) NOT NULL,
    created_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    updated_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT fk_cir_user FOREIGN KEY (user_id) REFERENCES users(user_id),
    CONSTRAINT fk_cir_range FOREIGN KEY (card_range_id) REFERENCES card_ranges(card_range_id),
    CONSTRAINT fk_cir_batch FOREIGN KEY (batch_id) REFERENCES card_issuance_batches(card_issuance_batch_id),
    CONSTRAINT ck_cir_status CHECK (
        status IN ('PENDING_EXPORT', 'EXPORTED', 'PROCESSING_RESULT', 'ISSUED', 'REJECTED', 'RECOVERY_REQUIRED')
    )
);

CREATE UNIQUE INDEX uq_cir_one_open_per_user_range
    ON card_issuance_requests (
        CASE WHEN status IN ('PENDING_EXPORT', 'EXPORTED', 'PROCESSING_RESULT', 'RECOVERY_REQUIRED') THEN user_id END,
        CASE WHEN status IN ('PENDING_EXPORT', 'EXPORTED', 'PROCESSING_RESULT', 'RECOVERY_REQUIRED') THEN card_range_id END
    );

CREATE INDEX idx_cir_export_queue
    ON card_issuance_requests(status, created_at, card_issuance_request_id);

CREATE TABLE card_issuance_request_providers (
    card_issuance_request_id RAW(16) NOT NULL,
    provider_user_id RAW(16) NOT NULL,
    provider_id RAW(16) NOT NULL,
    enrollment_id RAW(16) NOT NULL UNIQUE,
    enrollment_order NUMBER(10,0) NOT NULL,
    status VARCHAR2(32) NOT NULL,
    created_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    updated_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT pk_cirp PRIMARY KEY (card_issuance_request_id, provider_id),
    CONSTRAINT fk_cirp_request FOREIGN KEY (card_issuance_request_id) REFERENCES card_issuance_requests(card_issuance_request_id),
    CONSTRAINT fk_cirp_provider_user FOREIGN KEY (provider_user_id) REFERENCES provider_users(provider_user_id),
    CONSTRAINT fk_cirp_provider FOREIGN KEY (provider_id) REFERENCES providers(provider_id),
    CONSTRAINT ck_cirp_status CHECK (status IN ('PENDING', 'PROVISIONING', 'ACTIVE', 'REJECTED', 'RECOVERY_REQUIRED'))
);

CREATE TABLE card_issuance_batch_rows (
    card_issuance_batch_id RAW(16) NOT NULL,
    card_issuance_request_id RAW(16) NOT NULL,
    row_number NUMBER(10,0) NOT NULL,
    result_status VARCHAR2(32),
    safe_result_code VARCHAR2(128),
    safe_result_message VARCHAR2(1000),
    processed_at TIMESTAMP(6) WITH TIME ZONE,
    CONSTRAINT pk_cibr PRIMARY KEY (card_issuance_batch_id, card_issuance_request_id),
    CONSTRAINT uq_cibr_row UNIQUE (card_issuance_batch_id, row_number),
    CONSTRAINT fk_cibr_batch FOREIGN KEY (card_issuance_batch_id) REFERENCES card_issuance_batches(card_issuance_batch_id),
    CONSTRAINT fk_cibr_request FOREIGN KEY (card_issuance_request_id) REFERENCES card_issuance_requests(card_issuance_request_id),
    CONSTRAINT ck_cibr_status CHECK (
        result_status IS NULL OR result_status IN ('ISSUED', 'REJECTED', 'RECOVERY_REQUIRED', 'FAILED')
    )
);
