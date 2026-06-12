-- sqlx database drop -y -f
-- sqlx database create
-- sqlx migrate run

-- Add migration script here
-- ==========================================
-- 0. ENUM DEFINITIONS
-- ==========================================

CREATE TYPE audit_action_enum AS ENUM ('INSERT', 'UPDATE', 'DELETE');
CREATE TYPE account_status_enum AS ENUM ('ACTIVE', 'INACTIVE', 'SUSPENDED', 'BLOCKED');
CREATE TYPE currency_enum AS ENUM ('IRR', 'USD', 'EUR');
CREATE TYPE settlement_status_enum AS ENUM ('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED', 'CANCELLED');
CREATE TYPE transaction_type_enum AS ENUM ('CREDIT', 'DEBIT', 'TRANSFER', 'FEE', 'SETTLEMENT', 'REFUND');
CREATE TYPE transaction_status_enum AS ENUM ('PENDING', 'SUCCESS', 'FAILED', 'REVERSED');

-- New Enums for Priority Management
CREATE TYPE priority_status_enum AS ENUM ('ACTIVE', 'CONSUMED', 'EXPIRED', 'CANCELLED');
CREATE TYPE priority_usage_type_enum AS ENUM ('SINGLE_USE', 'MULTI_USE');

-- ==========================================
-- 1. AUDIT SYSTEM (Functions & Tables)
-- ==========================================

CREATE TABLE audit_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_type VARCHAR(50) NOT NULL,
    entity_id UUID NOT NULL,
    actor_id UUID NOT NULL,
    action_type audit_action_enum NOT NULL,
    old_values JSONB,
    new_values JSONB,
    ip_address INET,
    user_agent TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_audit_entity ON audit_logs(entity_type, entity_id);

CREATE OR REPLACE FUNCTION fn_audit_trigger()
RETURNS TRIGGER AS $$
DECLARE
    current_actor UUID;
BEGIN
    BEGIN
        current_actor := current_setting('app.current_user_id', true)::UUID;
    EXCEPTION WHEN OTHERS THEN
        current_actor := NULL;
    END;

    IF current_actor IS NULL THEN
        RAISE EXCEPTION 'Audit Log Error: app.current_user_id must be set in the current transaction.';
    END IF;

    IF (TG_OP = 'INSERT') THEN
        INSERT INTO audit_logs (entity_type, entity_id, actor_id, action_type, new_values)
        VALUES (TG_TABLE_NAME, NEW.id, current_actor, 'INSERT'::audit_action_enum, to_jsonb(NEW));
        RETURN NEW;
    ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO audit_logs (entity_type, entity_id, actor_id, action_type, old_values, new_values)
        VALUES (TG_TABLE_NAME, OLD.id, current_actor, 'UPDATE'::audit_action_enum, to_jsonb(OLD), to_jsonb(NEW));
        RETURN NEW;
    ELSIF (TG_OP = 'DELETE') THEN
        INSERT INTO audit_logs (entity_type, entity_id, actor_id, action_type, old_values)
        VALUES (TG_TABLE_NAME, OLD.id, current_actor, 'DELETE'::audit_action_enum, to_jsonb(OLD));
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;


-- ==========================================
-- 2. CORE ENTITIES (Providers & Users)
-- ==========================================

CREATE TABLE providers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    is_core BOOLEAN NOT NULL DEFAULT FALSE, 
    
    legal_name VARCHAR(255) NOT NULL,
    trade_name VARCHAR(255) NOT NULL,
    tax_id VARCHAR(50) NOT NULL,
    email_address VARCHAR(255) NOT NULL,
    office_phone VARCHAR(50) NOT NULL,
    website_url VARCHAR(255),
    mailing_address TEXT NOT NULL,
    alert_phone_numbers JSONB NOT NULL DEFAULT '[]',
    banner_image_id VARCHAR(100),
    profile_image_id VARCHAR(100),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    
    fee_rate_bps INTEGER NOT NULL DEFAULT 0, 
    fixed_fee_amount BIGINT NOT NULL DEFAULT 0, 
    
    kafka_config JSONB NOT NULL DEFAULT '{}',
    
    ledger_account_id UUID NOT NULL UNIQUE,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_only_one_core_provider ON providers(is_core) WHERE is_core = TRUE;

CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    nid VARCHAR(10) UNIQUE NOT NULL, 
    internal_metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


-- ==========================================
-- 3. RELATIONAL & METADATA MAPPINGS
-- ==========================================

CREATE TABLE user_providers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(), 
    user_id UUID NOT NULL REFERENCES users(id),
    provider_id UUID NOT NULL REFERENCES providers(id),
    external_metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, provider_id) 
);

CREATE TABLE user_accounts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id),
    provider_id UUID NOT NULL REFERENCES providers(id), 
    ledger_account_id UUID NOT NULL UNIQUE,
    status account_status_enum DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, provider_id) 
);


-- ==========================================
-- 4. PROVIDER FINANCIAL SETTINGS
-- ==========================================

CREATE TABLE provider_bank_accounts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    provider_id UUID NOT NULL REFERENCES providers(id),
    bank_name VARCHAR(100) NOT NULL,
    account_holder_name VARCHAR(255) NOT NULL,
    account_number VARCHAR(100) NOT NULL,
    sheba_number VARCHAR(26),
    card_number VARCHAR(16),
    currency currency_enum DEFAULT 'IRR',
    is_default BOOLEAN DEFAULT FALSE,
    status account_status_enum DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE provider_liquidity_limits (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(), 
    provider_id UUID UNIQUE NOT NULL REFERENCES providers(id),
    max_credit_limit BIGINT NOT NULL,
    warning_threshold_amount BIGINT NOT NULL,
    auto_block_on_exceed BOOLEAN DEFAULT TRUE,
    last_alert_sent_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE provider_settlements (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    provider_id UUID NOT NULL REFERENCES providers(id),
    provider_bank_account_id UUID REFERENCES provider_bank_accounts(id),
    amount BIGINT NOT NULL,
    currency currency_enum DEFAULT 'IRR',
    status settlement_status_enum NOT NULL DEFAULT 'PENDING',
    ledger_transfer_id UUID,
    bank_reference_id VARCHAR(255),
    external_request_id VARCHAR(100) UNIQUE,
    description TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    processed_at TIMESTAMPTZ
);


-- ==========================================
-- 5. USER BALANCE PRIORITIES (Master/Detail)
-- ==========================================

CREATE TABLE user_priority_configs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id),
    idempotency_key VARCHAR(255) UNIQUE NOT NULL,
    status priority_status_enum NOT NULL DEFAULT 'ACTIVE',
    expires_at TIMESTAMPTZ,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    deleted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Ensure only one ACTIVE config per user at a time
CREATE UNIQUE INDEX idx_user_active_priority_config ON user_priority_configs(user_id) WHERE status = 'ACTIVE';
-- Index for finding expired configurations efficiently
CREATE INDEX idx_priority_configs_expires_at ON user_priority_configs(expires_at) WHERE expires_at IS NOT NULL;

CREATE TABLE user_priority_items (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    config_id UUID NOT NULL REFERENCES user_priority_configs(id) ON DELETE CASCADE,
    provider_id UUID NOT NULL REFERENCES providers(id),
    ledger_account_id UUID NOT NULL,
    priority_order INTEGER NOT NULL CHECK (priority_order > 0),
    usage_type priority_usage_type_enum NOT NULL DEFAULT 'SINGLE_USE',
    
    max_amount BIGINT NOT NULL CHECK (max_amount >= 0),
    
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    deleted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Ensure a provider is not added twice in the same priority configuration
    UNIQUE (config_id, provider_id),
    -- Ensure priority orders are unique within a configuration
    UNIQUE (config_id, priority_order)
);


-- ==========================================
-- 6. TRANSACTIONS (Double-entry)
-- ==========================================

CREATE TABLE transactions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key VARCHAR(100) UNIQUE NOT NULL,
    transaction_type transaction_type_enum NOT NULL, 
    
    amount BIGINT NOT NULL CHECK (amount > 0),
    currency currency_enum DEFAULT 'IRR',
    
    dr_account_id UUID NOT NULL, 
    cr_account_id UUID NOT NULL, 
    
    user_id UUID REFERENCES users(id),
    provider_id UUID REFERENCES providers(id),
    
    -- Link to priority item if this transaction consumed a priority balance
    priority_item_id UUID REFERENCES user_priority_items(id),
    
    status transaction_status_enum NOT NULL DEFAULT 'PENDING',
    parent_transaction_id UUID REFERENCES transactions(id),
    external_reference_id VARCHAR(255),
    description TEXT,
    metadata JSONB NOT NULL DEFAULT '{}',
    
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ
);

-- Index for fast lookup of transactions related to a specific priority item
CREATE INDEX idx_transactions_priority_item ON transactions(priority_item_id) WHERE priority_item_id IS NOT NULL;


-- ==========================================
-- 7. SYSTEM ACCOUNTS (Chart of Accounts)
-- ==========================================
CREATE TYPE system_account_category_enum AS ENUM ('REVENUE', 'LIABILITY', 'ASSET', 'EXPENSE', 'SUSPENSE');
CREATE TABLE system_accounts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL, 
    category system_account_category_enum NOT NULL,
    
    ledger_account_id UUID NOT NULL UNIQUE, 
    
    currency currency_enum DEFAULT 'IRR',
    description TEXT,
    
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TRIGGER trg_audit_system_accounts 
AFTER INSERT OR UPDATE OR DELETE ON system_accounts 
FOR EACH ROW EXECUTE FUNCTION fn_audit_trigger();

-- ==========================================
-- 8. AUDIT TRIGGERS APPLICATION
-- ==========================================

CREATE TRIGGER trg_audit_providers AFTER INSERT OR UPDATE OR DELETE ON providers FOR EACH ROW EXECUTE FUNCTION fn_audit_trigger();
CREATE TRIGGER trg_audit_users AFTER INSERT OR UPDATE OR DELETE ON users FOR EACH ROW EXECUTE FUNCTION fn_audit_trigger();
CREATE TRIGGER trg_audit_user_providers AFTER INSERT OR UPDATE OR DELETE ON user_providers FOR EACH ROW EXECUTE FUNCTION fn_audit_trigger();
CREATE TRIGGER trg_audit_user_accounts AFTER INSERT OR UPDATE OR DELETE ON user_accounts FOR EACH ROW EXECUTE FUNCTION fn_audit_trigger();
CREATE TRIGGER trg_audit_bank_accounts AFTER INSERT OR UPDATE OR DELETE ON provider_bank_accounts FOR EACH ROW EXECUTE FUNCTION fn_audit_trigger();
CREATE TRIGGER trg_audit_limits AFTER INSERT OR UPDATE OR DELETE ON provider_liquidity_limits FOR EACH ROW EXECUTE FUNCTION fn_audit_trigger();
CREATE TRIGGER trg_audit_settlements AFTER INSERT OR UPDATE OR DELETE ON provider_settlements FOR EACH ROW EXECUTE FUNCTION fn_audit_trigger();
CREATE TRIGGER trg_audit_transactions AFTER INSERT OR UPDATE OR DELETE ON transactions FOR EACH ROW EXECUTE FUNCTION fn_audit_trigger();

-- Audit triggers for priority tables
CREATE TRIGGER trg_audit_priority_configs AFTER INSERT OR UPDATE OR DELETE ON user_priority_configs FOR EACH ROW EXECUTE FUNCTION fn_audit_trigger();
CREATE TRIGGER trg_audit_priority_items AFTER INSERT OR UPDATE OR DELETE ON user_priority_items FOR EACH ROW EXECUTE FUNCTION fn_audit_trigger();
