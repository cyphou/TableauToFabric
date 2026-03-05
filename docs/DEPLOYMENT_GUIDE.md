# Deployment Guide — Microsoft Fabric

This guide covers deploying generated artifacts to Microsoft Fabric workspaces using the tool's built-in deployment pipeline.

---

## Prerequisites

1. **Microsoft Fabric workspace** with capacity assigned
2. **Azure AD App Registration** with the following API permissions:
   - `Power BI Service` → `Dataset.ReadWrite.All`
   - `Power BI Service` → `Report.ReadWrite.All`
   - `Power BI Service` → `Workspace.ReadWrite.All`
3. **Admin consent** granted for the above permissions
4. **Client secret** created for the app registration

## Environment Variables

Create a `.env` file (or set environment variables) based on `.env.example`:

```bash
# Required
FABRIC_WORKSPACE_ID=<your-workspace-guid>
FABRIC_TENANT_ID=<your-azure-ad-tenant-guid>
FABRIC_CLIENT_ID=<your-app-registration-client-id>
FABRIC_CLIENT_SECRET=<your-client-secret>

# Optional
FABRIC_API_BASE_URL=https://api.fabric.microsoft.com/v1
USE_MANAGED_IDENTITY=false
LOG_LEVEL=INFO
LOG_FORMAT=text
DEPLOYMENT_TIMEOUT=300
RETRY_ATTEMPTS=3
RETRY_DELAY=5
```

## Authentication Methods

```
  ┌───────────────────────────────────────────────────┐
  │                Authentication Flow                │
  ├───────────────────────────────────────────────────┤
  │                                                   │
  │  ┌─────────────────┐    ┌──────────────────────┐  │
  │  │ Service         │    │ Azure AD             │  │
  │  │ Principal       ├───>│ Token Endpoint       │  │
  │  │ (CI/CD)         │    │ oauth2/v2.0/token    │  │
  │  └─────────────────┘    └──────────┬───────────┘  │
  │                                    │              │
  │  ┌─────────────────┐               v              │
  │  │ Managed         │    ┌──────────────────────┐  │
  │  │ Identity        ├───>│ Bearer Token         │  │
  │  │ (Azure-hosted)  │    └──────────┬───────────┘  │
  │  └─────────────────┘               │              │
  │                                    v              │
  │                         ┌──────────────────────┐  │
  │                         │ Fabric REST API      │  │
  │                         │ api.fabric.microsoft │  │
  │                         │ .com/v1              │  │
  │                         └──────────────────────┘  │
  └───────────────────────────────────────────────────┘
```

### Service Principal (Recommended for CI/CD)

Set `FABRIC_TENANT_ID`, `FABRIC_CLIENT_ID`, and `FABRIC_CLIENT_SECRET`. The tool uses `azure-identity`'s `ClientSecretCredential`.

```bash
pip install azure-identity
```

### Managed Identity (For Azure-hosted runners)

Set `USE_MANAGED_IDENTITY=true`. Uses `DefaultAzureCredential` which automatically picks up managed identity credentials.

### No Authentication (Local Dev)

Skip the deployment step and open generated `.pbip` files directly in Power BI Desktop, or manually import artifacts into a Fabric workspace.

## Deployment Pipeline

### Manual Deployment

```bash
# Generate the project
python migrate.py workbook.twbx --output-dir ./output

# Deploy to Fabric (requires azure-identity and requests)
python -c "
from fabric_import.deployer import FabricDeployer
deployer = FabricDeployer()
report = deployer.deploy_artifacts_batch('./output')
print(report.summary())
"
```

### CI/CD Deployment (GitHub Actions)

The project includes a 5-stage CI/CD pipeline in `.github/workflows/ci.yml`:

```
  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌───────────┐    ┌───────────┐
  │  1.Lint  │───>│  2.Test  │───>│3.Validate│───>│ 4.Deploy  │───>│ 5.Deploy  │
  │          │    │          │    │          │    │  Staging  │    │Production │
  │ flake8   │    │ pytest   │    │ migrate  │    │           │    │           │
  │ ruff     │    │ 3.9-3.12 │    │ + verify │    │ develop   │    │ main      │
  │          │    │          │    │ samples  │    │ branch    │    │ branch    │
  └──────────┘    └──────────┘    └──────────┘    └───────────┘    └───────────┘
       │               │               │                │                │
       v               v               v                v                v
   Code quality    All 1017+      Artifact gen      Auto-deploy      Auto-deploy
   checks          tests pass    + validation      on push           on push
```

1. **Lint** — flake8 + ruff
2. **Test** — pytest on Python 3.9–3.12+
3. **Validate** — migrate all sample .twb files, validate artifacts
4. **Deploy Staging** — auto-deploys on `develop` branch push
5. **Deploy Production** — auto-deploys on `main` branch push

#### GitHub Secrets Required

| Secret | Description |
|--------|------------|
| `FABRIC_WORKSPACE_ID` | Target Fabric workspace GUID |
| `FABRIC_TENANT_ID` | Azure AD tenant GUID |
| `FABRIC_CLIENT_ID` | App registration client ID |
| `FABRIC_CLIENT_SECRET` | App registration client secret |
| `STAGING_WORKSPACE_ID` | Staging workspace GUID (for staging deploy) |

### PowerShell Deployment Scripts

The `scripts/` directory contains PowerShell automation:

- **`Deploy-TableauMigration.ps1`** — End-to-end deployment script
- **`New-MigrationWorkspace.ps1`** — Create a migration workspace
- **`Validate-Deployment.ps1`** — Validate deployment results

## Environment Configurations

Three pre-configured environments in `fabric_import/config/environments.py`:

| Setting | Development | Staging | Production |
|---------|------------|---------|------------|
| Log level | DEBUG | INFO | WARNING |
| Timeout (s) | 120 | 300 | 600 |
| Retry attempts | 1 | 3 | 5 |
| Retry delay (s) | 1 | 5 | 10 |
| Approval required | No | No | Yes |

## Fabric Artifact Deployment Order

Deploy artifacts in this order:

```
  ┌────────────┐     ┌────────────┐     ┌────────────┐     ┌────────────┐     ┌────────────┐     ┌────────────┐
  │ 1.Lakehouse│────>│ 2.Dataflow │────>│ 3.Notebook │────>│ 4.Semantic │────>│ 5.Report   │────>│ 6.Pipeline │
  │            │     │    Gen2    │     │  (PySpark) │     │   Model    │     │   (.pbip)  │     │  (orch.)   │
  └────────────┘     └────────────┘     └────────────┘     └────────────┘     └────────────┘     └────────────┘
        │                  │                  │                  │                  │                  │
        v                  v                  v                  v                  v                  v
   Create Delta       Load data          Transform &       Point to            Bind visuals      Wire Dataflow
   table storage      from sources       compute calc      Lakehouse via       to Semantic       + Notebook +
                      into Lakehouse     columns           DirectLake          Model             Refresh
```

1. **Lakehouse** — Create the Lakehouse first (it holds your data)
2. **Dataflow Gen2** — Import data into Lakehouse Delta tables
3. **Notebook** — Run PySpark transformations (optional, depends on ETL strategy)
4. **Semantic Model** — Deploy TMDL model pointing to Lakehouse
5. **Data Pipeline** — Orchestrate Dataflow + Notebook
6. **Power BI Report** — Deploy .pbip report referencing the Semantic Model

## Retry & Error Handling

The `FabricClient` includes built-in retry logic:

- **HTTP 429** (Rate Limited): Respects `Retry-After` header, waits and retries
- **HTTP 5xx** (Server Error): Retries up to `RETRY_ATTEMPTS` times with `RETRY_DELAY` between attempts
- **Timeout**: Operations time out after `DEPLOYMENT_TIMEOUT` seconds

## Validation Before Deployment

Always validate generated artifacts before deploying:

```bash
python -c "
from fabric_import.validator import ArtifactValidator
results = ArtifactValidator.validate_directory('./output')
for name, result in results.items():
    status = 'OK' if result['valid'] else 'FAIL'
    print(f'{status}: {name} ({result[\"files_checked\"]} files, {len(result[\"warnings\"])} warnings)')
"
```

## Troubleshooting

| Issue | Solution |
|-------|---------|
| `401 Unauthorized` | Check tenant ID, client ID, and client secret. Ensure admin consent is granted. |
| `403 Forbidden` | Verify the service principal has workspace access (Admin/Member role). |
| `429 Too Many Requests` | Retry logic handles this automatically. Reduce batch size if persistent. |
| `ImportError: azure-identity` | Install with `pip install azure-identity`. Required for authentication. |
| `ImportError: requests` | Install with `pip install requests`. Falls back to `urllib` if not available. |
| Stale files on Windows | OneDrive may lock generated files. Close OneDrive sync or use `--output-dir` outside synced folders. |
