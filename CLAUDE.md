# php-kbc-project-restore – AI Development Context

## What this repository does

Low-level PHP library for restoring a Keboola project from a backup in cloud storage. Used by `app-project-restore` as its restore engine.

## Documentation

- **`docs/overview.md`** – public methods, worker-create-table.php, what is skipped, helper classes
- **`docs/how-it-works.md`** – step-by-step restore flow (3 phases of restoreTables, interleaveByBucket, getDataFromStorage per backend)

## Required environment variables

Before running tests, verify that these variables are present in `.env` (or exported in the shell). If missing, ask for them explicitly.

**For AWS S3 tests:**

| Variable | Description |
|---|---|
| `TEST_STORAGE_API_URL` | Keboola project URL (destination) |
| `TEST_STORAGE_API_TOKEN` | Project storage token (destination) |
| `TEST_AWS_ACCESS_KEY_ID` | AWS Access Key ID |
| `TEST_AWS_SECRET_ACCESS_KEY` | AWS Secret Access Key |
| `TEST_AWS_REGION` | AWS region (e.g. `us-east-1`) |
| `TEST_AWS_S3_BUCKET` | S3 bucket name containing the backup |

**For Azure ABS tests:**

| Variable | Description |
|---|---|
| `TEST_AZURE_ACCOUNT_NAME` | Azure storage account name |
| `TEST_AZURE_ACCOUNT_KEY` | Azure storage key |
| `TEST_AZURE_CONTAINER_NAME` | Azure Blob Storage container name containing the backup |

**For GCS tests:**

| Variable | Description |
|---|---|
| `TEST_GCP_SERVICE_ACCOUNT` | JSON service account key (full JSON as string) |
| `TEST_GCP_BUCKET` | GCS bucket name containing the backup |

> Check that the `.env` file exists in the repo root. If not, create it based on the list above.

## Development commands

Service name in `docker-compose.yml` is `tests`. Note: `composer tests` runs **only** `tests-abs` and `tests-s3` – GCS tests are run separately via `tests-gcs`.

```bash
docker compose run --rm tests composer phpcs
docker compose run --rm tests composer phpstan
docker compose run --rm tests composer tests              # tests-abs + tests-s3 (WITHOUT GCS)
docker compose run --rm tests composer tests-abs          # Azure Blob Storage
docker compose run --rm tests composer tests-s3           # AWS S3
docker compose run --rm tests composer tests-gcs          # Google Cloud Storage
docker compose run --rm tests composer tests-cross-backends
docker compose run --rm tests composer tests-phpunit      # direct phpunit call (without data preparation)
docker compose run --rm tests composer prepare-test-data  # test data preparation
docker compose run --rm tests composer build              # phplint + phpcs + phpstan + tests
docker compose run --rm tests composer ci-except-tests    # build without running tests
```

## Key files

| File | Purpose |
|---|---|
| `src/Restore.php` | Abstract class – all restore logic |
| `src/S3Restore.php` | AWS S3 implementation |
| `src/AbsRestore.php` | Azure Blob Storage implementation |
| `src/GcsRestore.php` | Google Cloud Storage implementation |
| `src/worker-create-table.php` | Child process for parallel table creation |
| `src/StorageApi/Token.php` | Token permission validation |
| `src/StorageApi/BucketInfo.php` | Working with bucket information from backup |
| `src/StorageApi/ConfigurationCorrector.php` | Configuration correction after migration (component ID translations) |
| `src/StorageApi/StackSpecificComponentIdTranslator.php` | Component ID translation between stacks |

## worker-create-table.php

Child process launched via `symfony/process`. Accepts JSON via STDIN, creates one table in Storage API (typed or untyped), returns JSON with result or error.

Special error flags: `isNullablePkError` (PK column is nullable – resolved by `forcePrimaryKeyNotNull`), `isClientException`.

## Architecture

Abstract class `Restore` + concrete implementations for each backend. Parallel table creation via worker scripts.

## Coding standards

- PHP 8.x with strict types
- PHPStan level max
- Keboola coding standard (PSR-12)

## Related repositories

- Used in: `app-project-restore`
- Paired with: `php-kbc-project-backup`
