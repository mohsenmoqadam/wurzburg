CREATE TABLE schema_migrations (
    version VARCHAR2(64) PRIMARY KEY,
    description VARCHAR2(255) NOT NULL,
    checksum VARCHAR2(128) NOT NULL,
    applied_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
);

CREATE TABLE oracle_migration_locks (
    lock_name VARCHAR2(64) PRIMARY KEY,
    locked_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
);

INSERT INTO oracle_migration_locks (lock_name) VALUES ('WURZBURG_SCHEMA_MIGRATION');

CREATE TABLE card_range_allocation_locks (
    lock_name VARCHAR2(64) PRIMARY KEY,
    created_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
);

INSERT INTO card_range_allocation_locks (lock_name) VALUES ('CARD_RANGE_STRUCTURE');

CREATE TABLE idempotency_records (
    idempotency_record_id RAW(16) PRIMARY KEY,
    operation_type VARCHAR2(100) NOT NULL,
    idempotency_key VARCHAR2(255) NOT NULL,
    request_hash VARCHAR2(128) NOT NULL,
    status VARCHAR2(32) DEFAULT 'IN_PROGRESS' NOT NULL,
    resource_type VARCHAR2(100),
    resource_id RAW(16),
    response_snapshot JSON,
    error_snapshot JSON,
    created_by_subject VARCHAR2(255) NOT NULL,
    created_by_client_id VARCHAR2(255),
    actor_provider_id RAW(16),
    actor_user_id RAW(16),
    correlation_id VARCHAR2(128) NOT NULL,
    request_id VARCHAR2(128) NOT NULL,
    created_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    updated_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    completed_at TIMESTAMP(6) WITH TIME ZONE,
    CONSTRAINT uq_idempotency_operation_key UNIQUE (operation_type, idempotency_key),
    CONSTRAINT ck_idempotency_status CHECK (
        status IN ('IN_PROGRESS', 'COMPLETED', 'FAILED', 'CONFLICT')
    )
);

CREATE TABLE audit_logs (
    audit_log_id RAW(16) PRIMARY KEY,
    entity_type VARCHAR2(100) NOT NULL,
    entity_id RAW(16) NOT NULL,
    action_type VARCHAR2(64) NOT NULL,
    reason VARCHAR2(1000),
    old_values JSON,
    new_values JSON,
    actor_subject VARCHAR2(255) NOT NULL,
    actor_client_id VARCHAR2(255),
    actor_provider_id RAW(16),
    actor_user_id RAW(16),
    actor_issuer VARCHAR2(255),
    source_ip VARCHAR2(64),
    correlation_id VARCHAR2(128) NOT NULL,
    request_id VARCHAR2(128) NOT NULL,
    created_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT ck_audit_action CHECK (
        action_type IN ('INSERT', 'UPDATE', 'DELETE', 'STATE_TRANSITION', 'SECRET_READ', 'SECRET_ROTATE')
    )
);

CREATE TABLE operation_wal (
    operation_id RAW(16) PRIMARY KEY,
    operation_type VARCHAR2(100) NOT NULL,
    aggregate_type VARCHAR2(100) NOT NULL,
    aggregate_id RAW(16) NOT NULL,
    status VARCHAR2(32) NOT NULL,
    deterministic_external_id RAW(16),
    request_json JSON NOT NULL,
    response_json JSON,
    error_json JSON,
    attempt_count NUMBER(10,0) DEFAULT 0 NOT NULL,
    next_attempt_at TIMESTAMP(6) WITH TIME ZONE,
    locked_by VARCHAR2(255),
    locked_until TIMESTAMP(6) WITH TIME ZONE,
    created_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    updated_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    completed_at TIMESTAMP(6) WITH TIME ZONE,
    CONSTRAINT ck_operation_wal_status CHECK (
        status IN ('PENDING', 'EXTERNAL_IN_FLIGHT', 'EXTERNAL_VERIFIED', 'COMPLETED', 'FAILED', 'DEAD_LETTER')
    )
);

CREATE TABLE business_config (
    config_key VARCHAR2(255) PRIMARY KEY,
    value_json JSON NOT NULL,
    value_type VARCHAR2(64) NOT NULL,
    version NUMBER(19,0) NOT NULL,
    status VARCHAR2(32) NOT NULL,
    effective_at TIMESTAMP(6) WITH TIME ZONE,
    updated_by_subject VARCHAR2(255) NOT NULL,
    updated_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    description VARCHAR2(1000),
    CONSTRAINT ck_business_config_status CHECK (
        status IN ('DRAFT', 'ACTIVE', 'SUSPENDED', 'SUPERSEDED')
    )
);

CREATE TABLE business_config_audit (
    business_config_audit_id RAW(16) PRIMARY KEY,
    config_key VARCHAR2(255) NOT NULL,
    old_value_json JSON,
    new_value_json JSON,
    old_version NUMBER(19,0),
    new_version NUMBER(19,0) NOT NULL,
    changed_by_subject VARCHAR2(255) NOT NULL,
    changed_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    reason VARCHAR2(1000),
    CONSTRAINT fk_business_config_audit_key
        FOREIGN KEY (config_key) REFERENCES business_config(config_key)
);

CREATE TABLE integration_outbox (
    outbox_event_id RAW(16) PRIMARY KEY,
    operation_id RAW(16) NOT NULL UNIQUE,
    event_type VARCHAR2(150) NOT NULL,
    aggregate_type VARCHAR2(100) NOT NULL,
    aggregate_id RAW(16) NOT NULL,
    partition_key VARCHAR2(255) NOT NULL,
    payload_json JSON NOT NULL,
    headers_json JSON,
    status VARCHAR2(32) DEFAULT 'PENDING' NOT NULL,
    attempt_count NUMBER(10,0) DEFAULT 0 NOT NULL,
    next_attempt_at TIMESTAMP(6) WITH TIME ZONE,
    locked_by VARCHAR2(255),
    locked_until TIMESTAMP(6) WITH TIME ZONE,
    published_at TIMESTAMP(6) WITH TIME ZONE,
    dead_letter_reason VARCHAR2(1000),
    created_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    updated_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT ck_outbox_status CHECK (
        status IN ('PENDING', 'PUBLISHING', 'PUBLISHED', 'DEAD_LETTER')
    )
);

CREATE TABLE integration_inbox (
    inbox_event_id RAW(16) PRIMARY KEY,
    source_system VARCHAR2(100) NOT NULL,
    source_event_id RAW(16) NOT NULL,
    event_type VARCHAR2(150) NOT NULL,
    aggregate_type VARCHAR2(100) NOT NULL,
    aggregate_id RAW(16) NOT NULL,
    payload_json JSON NOT NULL,
    status VARCHAR2(32) DEFAULT 'RECEIVED' NOT NULL,
    received_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    processed_at TIMESTAMP(6) WITH TIME ZONE,
    error_json JSON,
    CONSTRAINT uq_inbox_source_event UNIQUE (source_system, source_event_id),
    CONSTRAINT ck_inbox_status CHECK (
        status IN ('RECEIVED', 'PROCESSED', 'FAILED')
    )
);

CREATE TABLE runtime_materialization_receipts (
    runtime_materialization_receipt_id RAW(16) PRIMARY KEY,
    receipt_event_id RAW(16) NOT NULL UNIQUE,
    operation_id RAW(16) NOT NULL,
    profile_type VARCHAR2(16) NOT NULL,
    aggregate_id RAW(16) NOT NULL,
    profile_id RAW(16),
    materialized_version NUMBER(19,0) NOT NULL,
    redis_key VARCHAR2(512) NOT NULL,
    materialized_at TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    received_at TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT uq_runtime_receipt_operation UNIQUE (operation_id, profile_type, materialized_version),
    CONSTRAINT ck_runtime_receipt_profile CHECK (
        profile_type IN ('CPOL', 'CRCTL', 'CP', 'FEE')
    )
);

CREATE INDEX idx_audit_entity ON audit_logs(entity_type, entity_id);
CREATE INDEX idx_audit_correlation ON audit_logs(correlation_id);
CREATE INDEX idx_idempotency_resource ON idempotency_records(resource_type, resource_id);
CREATE INDEX idx_operation_wal_lease ON operation_wal(status, next_attempt_at, locked_until);
CREATE INDEX idx_outbox_lease ON integration_outbox(status, next_attempt_at, locked_until);
CREATE INDEX idx_inbox_aggregate ON integration_inbox(aggregate_type, aggregate_id);
