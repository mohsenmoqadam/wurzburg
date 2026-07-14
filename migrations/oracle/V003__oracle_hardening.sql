-- Oracle production hardening for the card foundation.
-- This migration is intentionally additive because V002 may already be applied.

UPDATE card_ranges
SET metadata_json = '{}'
WHERE metadata_json IS NULL;

ALTER TABLE card_ranges
    MODIFY metadata_json DEFAULT '{}' NOT NULL;

CREATE TABLE card_range_allocation_locks (
    lock_name VARCHAR2(64) PRIMARY KEY
);

INSERT INTO card_range_allocation_locks (lock_name)
VALUES ('CARD_RANGE_ALLOCATION');

UPDATE card_range_providers
SET metadata_json = '{}'
WHERE metadata_json IS NULL;

ALTER TABLE card_range_providers
    MODIFY metadata_json DEFAULT '{}' NOT NULL;

ALTER TABLE card_policy_profiles
    ADD card_range_id RAW(16);

UPDATE card_policy_profiles cpp
SET card_range_id = (
    SELECT MAX(crpa.card_range_id)
    FROM card_range_policy_assignments crpa
    WHERE crpa.card_policy_profile_id = cpp.card_policy_profile_id
)
WHERE card_range_id IS NULL;

ALTER TABLE card_policy_profiles
    MODIFY card_range_id NOT NULL;

ALTER TABLE card_policy_profiles
    ADD CONSTRAINT fk_cpp_card_range
        FOREIGN KEY (card_range_id)
        REFERENCES card_ranges(card_range_id);

ALTER TABLE card_policy_profiles
    ADD CONSTRAINT uq_cpp_range_version
        UNIQUE (card_range_id, version);

CREATE INDEX idx_cpp_range_status
    ON card_policy_profiles(card_range_id, status);
