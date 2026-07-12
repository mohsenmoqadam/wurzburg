-- =============================================================================
-- Wurzburg Oracle application user/schema setup
-- =============================================================================
--
-- Purpose:
--   Creates the Wurzburg application schema and grants development privileges
--   required to create Wurzburg-owned tables, indexes, constraints, views,
--   sequences, procedures, triggers, and types.
--
-- Service expected by the application:
--   //87.247.175.207:1521/HYPERCARD
--
-- Security:
--   Do not commit real passwords. Replace CHANGE_ME with the environment
--   password approved by DBA/security before running this script.
--
-- Production hardening:
--   Review the development-only grants near the bottom before using this in
--   staging or production.
-- =============================================================================

-- 1. Create the application user/schema.
--    QUOTA is required for creating tables, indexes, and constraints in the
--    selected tablespace.
CREATE USER wurzburg_user IDENTIFIED BY "CHANGE_ME"
DEFAULT TABLESPACE users
QUOTA UNLIMITED ON users;

-- 2. Grant basic connection privilege.
GRANT CREATE SESSION TO wurzburg_user;

-- 3. Grant privileges for tables, indexes, and constraints.
--    CREATE TABLE allows creating indexes and constraints on the user's own
--    tables.
GRANT CREATE TABLE TO wurzburg_user;

-- 4. Grant privileges for views and sequences.
GRANT CREATE VIEW TO wurzburg_user;
GRANT CREATE SEQUENCE TO wurzburg_user;

-- 5. Grant privileges for PL/SQL programming.
GRANT CREATE PROCEDURE TO wurzburg_user;
GRANT CREATE TRIGGER TO wurzburg_user;

-- 6. Additional privileges useful for a development environment.
GRANT CREATE TYPE TO wurzburg_user;
GRANT CREATE SYNONYM TO wurzburg_user;
GRANT DEBUG CONNECT SESSION TO wurzburg_user;
GRANT SELECT ANY DICTIONARY TO wurzburg_user;
