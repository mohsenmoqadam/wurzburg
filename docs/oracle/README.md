# Oracle Setup Notes

This directory records Oracle operational setup required by Wurzburg.

## Application Service

Wurzburg expects the DBA to expose/register this Oracle service:

```text
Host: 87.247.175.207
Port: 1521
Service name: HYPERCARD
Connect string: //87.247.175.207:1521/HYPERCARD
```

The service name must exist on the Oracle listener. The application cannot
invent it locally.

## Application Schema

Current application schema/user:

```text
wurzburg_user
```

Do not commit real database passwords. Use environment-specific secret storage
or local untracked config for the actual password.

The reusable DBA script is:

```text
docs/oracle/create_wurzburg_user.sql
```

## Oracle Client Runtime

The Rust `oracle` crate uses ODPI-C and requires a 64-bit Oracle Client library
on the machine running Wurzburg.

If startup fails with:

```text
DPI-1047: Cannot locate a 64-bit Oracle Client library: "libclntsh.so: cannot open shared object file"
```

install Oracle Instant Client for Linux and make `libclntsh.so` visible to the
process. A typical setup is:

```text
Oracle Instant Client Basic or Basic Lite, 64-bit
LD_LIBRARY_PATH=/path/to/instantclient
```

### Ubuntu/Debian Development Setup

Ubuntu/Debian `apt` can install supporting packages, but Oracle Instant Client
itself should be downloaded from Oracle as the Linux x86-64 zip package.

```bash
sudo apt-get update
sudo apt-get install -y unzip libaio1
```

On newer Ubuntu/Debian releases, the package may be named `libaio1t64` instead:

```bash
sudo apt-get install -y unzip libaio1t64
```

Then install Oracle Instant Client Basic or Basic Lite:

```bash
sudo mkdir -p /opt/oracle
cd /opt/oracle
sudo unzip /path/to/instantclient-basic-linux.x64-*.zip
```

Register the Instant Client library path:

```bash
echo /opt/oracle/instantclient_* | sudo tee /etc/ld.so.conf.d/oracle-instantclient.conf
sudo ldconfig
```

Confirm that `libclntsh.so` is visible:

```bash
ldconfig -p | grep libclntsh
```

If you do not want to use `ldconfig`, set `LD_LIBRARY_PATH` before running
Wurzburg:

```bash
export LD_LIBRARY_PATH=/opt/oracle/instantclient_23_*/:$LD_LIBRARY_PATH
```

After installation, restart the shell/service that runs Wurzburg so it receives
the updated dynamic library path.

### Oracle Linux / RPM-Based Setup

On Oracle Linux, Oracle documents installing Instant Client through `dnf` after
enabling the Oracle Instant Client repository:

```bash
sudo dnf install oracle-instantclient-release-26ai-el9
sudo dnf install oracle-instantclient-basic
```

For Ubuntu/Debian, prefer the zip-based install above rather than relying on an
unofficial apt package.

## Privilege Notes

Minimum application development privileges:

- `CREATE SESSION`: allow login.
- `CREATE TABLE`: allow Wurzburg-owned tables, indexes, and constraints.
- `CREATE VIEW`: support reporting and compatibility views.
- `CREATE SEQUENCE`: support future sequence-backed technical identifiers if needed.
- `CREATE PROCEDURE`: support migration-managed PL/SQL routines if needed.
- `CREATE TRIGGER`: support DB-level hooks if we later choose them.
- `CREATE TYPE`: support Oracle object/collection types if ever needed.

Development-only privileges:

- `CREATE SYNONYM`
- `DEBUG CONNECT SESSION`
- `SELECT ANY DICTIONARY`

For staging/production, review development-only privileges with DBA/security
before granting them. Wurzburg should run with the least privileges that still
allow its managed migrations and runtime behavior.

## Tablespace

The schema currently uses:

```text
DEFAULT TABLESPACE users
QUOTA UNLIMITED ON users
```

`QUOTA UNLIMITED` is convenient for development. Production may use an explicit
quota based on capacity planning.
