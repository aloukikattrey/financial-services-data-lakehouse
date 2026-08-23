# Oracle to Azure Databricks Connectivity Setup

## Purpose

This document records the development setup used to connect the local synthetic Oracle operational source to Azure Databricks. The local Oracle database is used only as synthetic project data; no employer/client data or credentials are stored in the repository.

## 1. Azure Databricks Foundation

The project uses an Azure Databricks workspace created in Central India under the Azure for Students subscription.

Configured components:

- Azure Databricks workspace: `financial-services-databricks`
- Workspace type: Hybrid
- Unity Catalog metastore: `databricks-finance-metastore`
- Metastore region: Central India
- Unity Catalog catalog: `databricks-cata`
- Serverless Starter Warehouse available for SQL
- Development all-purpose compute: `finance-oracle-dev`
- Databricks Runtime: 17.3 LTS
- Single-node development compute
- Photon disabled for the initial connectivity work
- Auto-termination configured to limit unnecessary trial compute usage

## 2. Unity Catalog Storage Foundation

The Unity Catalog metastore was created with the following managed-storage root:

```text
abfss://metastore@sginancialdb.dfs.core.windows.net/07983eb5-1316-4c88-af3e-4c6d078040f2
```

An Azure Databricks Access Connector was created and configured with a system-assigned managed identity.

Databricks storage credential:

```text
Name: aloukik_creds
```

The credential uses the existing Access Connector. It was initially present but was not assigned to the metastore as its root storage credential. This caused the Unity Catalog error:

```text
DAC_DOES_NOT_EXIST
Root storage credential for metastore does not exist
```

The metastore was then updated to use the correct Databricks storage credential object ID:

```text
Metastore ID:
07983eb5-1316-4c88-af3e-4c6d078040f2

Databricks storage credential ID:
a191924e-0d14-4fc7-a7ad-d5f43919b231

Storage credential name:
aloukik_creds
```

The final metastore configuration reports:

```text
storage_root_credential_name = aloukik_creds
storage_root_credential_id   = a191924e-0d14-4fc7-a7ad-d5f43919b231
```

This was required before creating Unity Catalog managed volumes.

## 3. Local Oracle Source

The source system is a synthetic Oracle Database Free instance running locally in Docker Desktop on Windows. This avoids creating an always-on Azure VM solely for the source database.

Oracle source layout:

```text
Oracle FREEPDB1
└── FINANCE_APP
    ├── CLIENT
    ├── ACCOUNT
    ├── PORTFOLIO
    ├── SECURITY
    ├── HOLDINGS
    └── TRN_TRANSACTIONS
```

The database contains synthetic financial-services data modeled on a transaction-and-holdings domain.

## 4. Local Oracle Networking

Oracle runs locally on TCP port `1521` and is published from Docker.

Local verification used:

```powershell
Test-NetConnection localhost -Port 1521
```

The test returned a successful TCP connection.

The Oracle listener was also verified as listening on port `1521`, with `FREE` and `FREEPDB1` services in READY status.

## 5. Development Tunnel

Because the Oracle database is running on a local Windows machine while Databricks is running in Azure, Databricks cannot reach `localhost:1521` directly.

For temporary development connectivity, a TCP reverse tunnel was created from the local machine to Pinggy.

Tunnel command:

```powershell
ssh -p 443 -R0:127.0.0.1:1521 tcp@free.pinggy.io
```

The tunnel produced a temporary public TCP endpoint. The exact endpoint is intentionally not recorded here because it is ephemeral and should not be treated as project configuration.

The tunnel window must remain open while the connectivity test is running.

### Security note

This tunnel is only for temporary development using synthetic data. It is not a production architecture and must not be used for real client/employer data or credentials.

A production architecture would use private network connectivity such as VPN/ExpressRoute rather than exposing an Oracle listener through a public tunnel.

## 6. Databricks TCP Connectivity Test

A Databricks Python notebook named:

```text
oracle_connectivity_test
```

was attached to the development compute and used to test TCP connectivity to the temporary tunnel endpoint.

Test pattern:

```python
import socket

host = "<temporary-tunnel-host>"
port = <temporary-tunnel-port>

sock = socket.create_connection((host, port), timeout=10)
print("TCP connection successful")
sock.close()
```

The test succeeded.

This proves the network path:

```text
Databricks compute
    ↓
Internet
    ↓
Temporary TCP tunnel
    ↓
Windows PC
    ↓
Docker port 1521
    ↓
Oracle listener
```

## 7. Unity Catalog Volume for JDBC Driver

A project schema was created:

```sql
CREATE SCHEMA IF NOT EXISTS `databricks-cata`.finance;
```

A managed volume for the Oracle JDBC driver was then created successfully:

```sql
CREATE VOLUME IF NOT EXISTS `databricks-cata`.finance.jdbc_drivers;
```

The resulting path is:

```text
/Volumes/databricks-cata/finance/jdbc_drivers/
```

The volume creation originally failed because the metastore did not have a root storage credential. After assigning `aloukik_creds` as the metastore root credential, the volume creation completed successfully.

The Oracle JDBC driver `ojdbc11.jar` was uploaded to the managed volume.

## 8. Oracle Foreign Connection and Federation

A Databricks Oracle connection was created using the temporary tunnel endpoint and the Oracle `FINANCE_APP` credentials. Credentials are not stored in this repository.

The Oracle service name used for the foreign connection/catalog is:

```text
FREEPDB1
```

The resulting foreign catalog is:

```text
oracle_finance_source_catalog
```

The catalog was configured through the Lakehouse Federation Oracle workflow. Access was restricted to the project owner for this development environment, and the catalog metadata comment identifies it as a read-only connection to the synthetic Oracle source.

The foreign catalog successfully exposed the Oracle source schema and tables in Databricks. Verified source objects include:

```text
oracle_finance_source_catalog
└── FINANCE_APP
    ├── CLIENT
    ├── ACCOUNT
    ├── PORTFOLIO
    ├── SECURITY
    ├── HOLDINGS
    └── TRN_TRANSACTIONS
```

The data was verified as visible and queryable from Databricks, confirming the end-to-end federation path.

Example catalog query pattern:

```sql
SELECT *
FROM oracle_finance_source_catalog.finance_app.CLIENT
LIMIT 10;
```

## 9. End-to-End Connectivity Milestone

The completed development path is now:

```text
Local Oracle Database
        │
        │ TCP :1521
        ▼
  Pinggy TCP Tunnel
        │
        ▼
 Azure Databricks
        │
        ▼
 Oracle Connection
        │
        ▼
 Oracle Foreign Catalog
        │
        ▼
oracle_finance_source_catalog
        │
        ▼
    FINANCE_APP
        │
        ├── CLIENT
        ├── ACCOUNT
        ├── PORTFOLIO
        ├── SECURITY
        ├── HOLDINGS
        └── TRN_TRANSACTIONS
```

This proves that the synthetic Oracle source can be reached from Azure Databricks and exposed through Unity Catalog as a federated read-only data source.

## 10. Next Phase — Oracle to Bronze

Federation is the source-access layer. The next step is to materialize the required Oracle datasets into the Lakehouse Bronze layer rather than treating the foreign catalog as the final data platform.

Planned work:

- [x] Upload `ojdbc11.jar`
- [x] Create Oracle connection in Databricks
- [x] Create Oracle foreign catalog
- [x] Verify Oracle schemas/tables are visible
- [x] Verify source data is queryable from Databricks
- [ ] Decide batch JDBC vs incremental extraction pattern
- [ ] Implement Oracle-to-Bronze ingestion
- [ ] Create Bronze Delta tables
- [ ] Add ingestion metadata
- [ ] Implement incremental extraction strategy
- [ ] Test repeatable loads
- [ ] Add data-quality checks for Bronze ingestion

### Architecture note

The temporary Pinggy tunnel is a development-only networking solution. It is not the intended production architecture. A production implementation would use private connectivity between the Oracle environment and Azure/Databricks, such as VPN or ExpressRoute, depending on the deployment model.
