CREATE TABLE providers (
    provider_id RAW(16) PRIMARY KEY,
    legal_name VARCHAR2(255) NOT NULL,
    trade_name VARCHAR2(255) NOT NULL,
    tax_id VARCHAR2(64),
    registration_number VARCHAR2(128),
    email_address VARCHAR2(255),
    website_url VARCHAR2(512),
    mailing_address VARCHAR2(2000),
    status VARCHAR2(32) DEFAULT 'PENDING_PROVISIONING' NOT NULL,
    metadata_json JSON DEFAULT '{}' NOT NULL,
    created_by_subject VARCHAR2(255) NOT NULL,
    updated_by_subject VARCHAR2(255) NOT NULL,
    created_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    updated_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT ck_providers_status CHECK (
        status IN ('PENDING_PROVISIONING', 'READY', 'ACTIVE', 'SUSPENDED', 'INACTIVE', 'FAILED')
    )
);

CREATE INDEX idx_providers_status_created
    ON providers(status, created_at, provider_id);

CREATE INDEX idx_providers_tax_id
    ON providers(tax_id);

CREATE INDEX idx_providers_registration_number
    ON providers(registration_number);

CREATE TABLE provider_contacts (
    provider_contact_id RAW(16) PRIMARY KEY,
    provider_id RAW(16) NOT NULL,
    contact_type VARCHAR2(32) NOT NULL,
    contact_name VARCHAR2(255),
    email VARCHAR2(255),
    phone VARCHAR2(64),
    mobile VARCHAR2(64),
    sms_enabled NUMBER(1) DEFAULT 0 NOT NULL,
    metadata_json JSON DEFAULT '{}' NOT NULL,
    status VARCHAR2(32) DEFAULT 'ACTIVE' NOT NULL,
    created_by_subject VARCHAR2(255) NOT NULL,
    updated_by_subject VARCHAR2(255) NOT NULL,
    created_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    updated_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT fk_provider_contacts_provider
        FOREIGN KEY (provider_id) REFERENCES providers(provider_id),
    CONSTRAINT ck_provider_contacts_type CHECK (
        contact_type IN ('FINANCE', 'TECHNICAL', 'OPERATIONS', 'SECURITY', 'NOTIFICATION', 'LEGAL')
    ),
    CONSTRAINT ck_provider_contacts_sms CHECK (sms_enabled IN (0, 1)),
    CONSTRAINT ck_provider_contacts_status CHECK (status IN ('ACTIVE', 'SUSPENDED'))
);

CREATE INDEX idx_provider_contacts_lookup
    ON provider_contacts(provider_id, contact_type, status);

CREATE TABLE provider_operational_profiles (
    provider_operational_profile_id RAW(16) PRIMARY KEY,
    provider_id RAW(16) NOT NULL,
    status VARCHAR2(32) NOT NULL,
    version NUMBER(19,0) NOT NULL,
    effective_at TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    profile_json JSON NOT NULL,
    superseded_by_profile_id RAW(16),
    created_by_subject VARCHAR2(255) NOT NULL,
    change_reason VARCHAR2(1000) NOT NULL,
    created_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT fk_pop_provider
        FOREIGN KEY (provider_id) REFERENCES providers(provider_id),
    CONSTRAINT fk_pop_superseded_by
        FOREIGN KEY (superseded_by_profile_id)
        REFERENCES provider_operational_profiles(provider_operational_profile_id),
    CONSTRAINT uq_pop_provider_version UNIQUE (provider_id, version),
    CONSTRAINT ck_pop_status CHECK (
        status IN ('SCHEDULED', 'ACTIVE', 'SUPERSEDED', 'CANCELLED')
    )
);

CREATE UNIQUE INDEX uq_pop_one_scheduled
    ON provider_operational_profiles (
        CASE WHEN status = 'SCHEDULED' THEN provider_id END
    );

CREATE UNIQUE INDEX uq_pop_one_active
    ON provider_operational_profiles (
        CASE WHEN status = 'ACTIVE' THEN provider_id END
    );

CREATE INDEX idx_pop_effective_lookup
    ON provider_operational_profiles(provider_id, status, effective_at);

CREATE TABLE provider_ledger_accounts (
    provider_ledger_account_id RAW(16) PRIMARY KEY,
    provider_id RAW(16) NOT NULL,
    account_category VARCHAR2(32) NOT NULL,
    tigerbeetle_account_id RAW(16) NOT NULL UNIQUE,
    status VARCHAR2(32) DEFAULT 'PROVISIONING' NOT NULL,
    metadata_json JSON DEFAULT '{}' NOT NULL,
    created_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    updated_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT fk_pla_provider
        FOREIGN KEY (provider_id) REFERENCES providers(provider_id),
    CONSTRAINT uq_pla_provider_category UNIQUE (provider_id, account_category),
    CONSTRAINT ck_pla_category CHECK (
        account_category IN ('PROVIDER_OWNED', 'PROVIDER_FEE', 'CMS_SETTLEMENT', 'PLATFORM_FEE')
    ),
    CONSTRAINT ck_pla_status CHECK (
        status IN ('PROVISIONING', 'ACTIVE', 'SUSPENDED', 'CLOSED', 'FAILED_PROVISIONING')
    )
);

CREATE INDEX idx_pla_provider_status
    ON provider_ledger_accounts(provider_id, status);

CREATE TABLE provider_kafka_access (
    provider_kafka_access_id RAW(16) PRIMARY KEY,
    provider_id RAW(16) NOT NULL UNIQUE,
    topic_name VARCHAR2(255) NOT NULL UNIQUE,
    username VARCHAR2(255) NOT NULL UNIQUE,
    consumer_group VARCHAR2(255) NOT NULL UNIQUE,
    security_protocol VARCHAR2(64) NOT NULL,
    sasl_mechanism VARCHAR2(64) NOT NULL,
    bootstrap_servers_json JSON NOT NULL,
    security_cert CLOB,
    credential_status VARCHAR2(32) DEFAULT 'PROVISIONING' NOT NULL,
    last_delivered_at TIMESTAMP(6) WITH TIME ZONE,
    rotated_at TIMESTAMP(6) WITH TIME ZONE,
    created_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    updated_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT fk_pka_provider
        FOREIGN KEY (provider_id) REFERENCES providers(provider_id),
    CONSTRAINT ck_pka_status CHECK (
        credential_status IN (
            'PROVISIONING', 'ACTIVE', 'ROTATING', 'SUSPENDING',
            'SUSPENDED', 'RESUMING', 'REVOKED', 'FAILED'
        )
    )
);

CREATE TABLE provider_kafka_credentials (
    provider_kafka_credential_id RAW(16) PRIMARY KEY,
    provider_kafka_access_id RAW(16) NOT NULL,
    provider_id RAW(16) NOT NULL,
    credential_version NUMBER(19,0) NOT NULL,
    password_ciphertext VARCHAR2(4000) NOT NULL,
    encryption_key_version VARCHAR2(128) NOT NULL,
    status VARCHAR2(32) DEFAULT 'CANDIDATE' NOT NULL,
    activated_at TIMESTAMP(6) WITH TIME ZONE,
    superseded_at TIMESTAMP(6) WITH TIME ZONE,
    revoked_at TIMESTAMP(6) WITH TIME ZONE,
    created_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    updated_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT fk_pkc_access
        FOREIGN KEY (provider_kafka_access_id)
        REFERENCES provider_kafka_access(provider_kafka_access_id),
    CONSTRAINT fk_pkc_provider
        FOREIGN KEY (provider_id) REFERENCES providers(provider_id),
    CONSTRAINT uq_pkc_provider_version UNIQUE (provider_id, credential_version),
    CONSTRAINT ck_pkc_status CHECK (
        status IN ('CANDIDATE', 'ACTIVE', 'SUPERSEDED', 'REVOKED', 'FAILED')
    )
);

CREATE UNIQUE INDEX uq_pkc_one_active
    ON provider_kafka_credentials (
        CASE WHEN status = 'ACTIVE' THEN provider_id END
    );

CREATE UNIQUE INDEX uq_pkc_one_candidate
    ON provider_kafka_credentials (
        CASE WHEN status = 'CANDIDATE' THEN provider_id END
    );

CREATE INDEX idx_pkc_access_history
    ON provider_kafka_credentials(provider_kafka_access_id, credential_version);

CREATE TABLE provider_provisioning_jobs (
    provider_provisioning_job_id RAW(16) PRIMARY KEY,
    provider_id RAW(16) NOT NULL,
    job_type VARCHAR2(32) NOT NULL,
    status VARCHAR2(32) DEFAULT 'PENDING' NOT NULL,
    attempt_count NUMBER(10,0) DEFAULT 0 NOT NULL,
    next_attempt_at TIMESTAMP(6) WITH TIME ZONE,
    locked_by VARCHAR2(255),
    locked_until TIMESTAMP(6) WITH TIME ZONE,
    error_code VARCHAR2(128),
    error_message VARCHAR2(2000),
    request_json JSON DEFAULT '{}' NOT NULL,
    result_json JSON DEFAULT '{}' NOT NULL,
    created_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    updated_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    completed_at TIMESTAMP(6) WITH TIME ZONE,
    CONSTRAINT fk_ppj_provider
        FOREIGN KEY (provider_id) REFERENCES providers(provider_id),
    CONSTRAINT ck_ppj_type CHECK (
        job_type IN (
            'TIGERBEETLE_PROVISION', 'KAFKA_PROVISION', 'KAFKA_ROTATE',
            'KAFKA_SUSPEND', 'KAFKA_RESUME'
        )
    ),
    CONSTRAINT ck_ppj_status CHECK (
        status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED')
    )
);

CREATE INDEX idx_ppj_claim
    ON provider_provisioning_jobs(status, next_attempt_at, locked_until);

CREATE UNIQUE INDEX uq_ppj_one_live_provider_type
    ON provider_provisioning_jobs (
        CASE WHEN status IN ('PENDING', 'RUNNING') THEN provider_id END,
        CASE WHEN status IN ('PENDING', 'RUNNING') THEN job_type END
    );

CREATE TABLE provider_event_subscriptions (
    provider_event_subscription_id RAW(16) PRIMARY KEY,
    provider_id RAW(16) NOT NULL,
    event_type VARCHAR2(128) NOT NULL,
    enabled NUMBER(1) DEFAULT 0 NOT NULL,
    version NUMBER(19,0) DEFAULT 1 NOT NULL,
    updated_by_subject VARCHAR2(255) NOT NULL,
    reason VARCHAR2(1000),
    created_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    updated_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT fk_pes_provider
        FOREIGN KEY (provider_id) REFERENCES providers(provider_id),
    CONSTRAINT uq_pes_provider_event UNIQUE (provider_id, event_type),
    CONSTRAINT ck_pes_enabled CHECK (enabled IN (0, 1))
);

CREATE INDEX idx_pes_provider_enabled
    ON provider_event_subscriptions(provider_id, enabled);
