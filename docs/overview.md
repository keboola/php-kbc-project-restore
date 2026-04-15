# php-kbc-project-restore – overview

## What the library does

Low-level PHP library for restoring a Keboola project from a backup in cloud storage. Contains no Keboola-specific boilerplate – it is pure restore logic.

Used by `app-project-restore` as its restore engine.

## Architecture

Abstract class `Restore` contains all logic. Concrete storage backends extend it:

```
Restore (abstract)
  ├─ S3Restore      – AWS S3
  ├─ AbsRestore     – Azure Blob Storage
  └─ GcsRestore     – Google Cloud Storage
```

A worker script exists for parallel table creation:

```
worker-create-table.php  – child process (symfony/process), one per table
```

## Public methods

| Method | What it does |
|---|---|
| `restoreProjectMetadata()` | Restores default branch metadata |
| `restoreBuckets(bool $checkBackend)` | Creates buckets; `checkBackend=false` ignores backend from backup |
| `restoreConfigs(array $skipComponents)` | Restores component configurations (except `skipComponents`) |
| `restoreTables(int $parallelism)` | Creates tables in parallel via worker scripts |
| `restoreTableAliases()` | Restores alias tables (call after `restoreTables`) |
| `restoreTriggers()` | Restores triggers |
| `restoreNotifications()` | Restores notification subscriptions |
| `restorePermanentFiles()` | Restores permanent files |

## Behavior configuration

| Method | Default | Description |
|---|---|---|
| `setDryRunMode(bool)` | `false` | Simulation – logs actions, creates nothing |
| `setForcePrimaryKeyNotNull(bool)` | `false` | Forces NOT NULL on PK columns |
| `setWorkerProcessFactory(Closure)` | – | Override factory for worker processes (for testing) |

## worker-create-table.php

Child process launched via `symfony/process`. Creates one table in Storage API.

**Input (STDIN, JSON):**

```json
{
  "autoloadPath": "...",
  "sapiUrl": "...",
  "sapiToken": "...",
  "runId": "...",
  "bucketId": "...",
  "tableName": "...",
  "columns": [...],
  "primaryKey": [...],
  "isTyped": true,
  "displayName": "...",
  "tableDefinition": {...}
}
```

**Output (JSON):**

```json
{
  "tableId": "in.c-bucket.table",
  "error": null,
  "code": null,
  "isNullablePkError": false,
  "isClientException": false
}
```

Special flags: `isNullablePkError` – PK column is nullable, resolved by `forcePrimaryKeyNotNull`; `isClientException` – client-side error (4xx), not a server error.

## Helper classes

| Class | Description |
|---|---|
| `StorageApi/Token.php` | Token permission validation |
| `StorageApi/BucketInfo.php` | Working with bucket information from backup |
| `StorageApi/ConfigurationCorrector.php` | Configuration correction after migration (component ID translations) |
| `StorageApi/StackSpecificComponentIdTranslator.php` | Component ID translation between stacks |

## Key files

| File | Description |
|---|---|
| `src/Restore.php` | Abstract class – all restore logic |
| `src/S3Restore.php` | AWS S3 implementation |
| `src/AbsRestore.php` | Azure Blob Storage implementation |
| `src/GcsRestore.php` | Google Cloud Storage implementation |
| `src/worker-create-table.php` | Child process for parallel table creation |
| `src/StorageApi/` | Storage API utilities |

## Development and testing

Service name in `docker-compose.yml` is `tests`. Note: `composer tests` does not run GCS tests – those are run separately via `tests-gcs`.

```bash
docker compose run --rm tests composer phpcs
docker compose run --rm tests composer phpstan
docker compose run --rm tests composer tests             # tests-abs + tests-s3 (WITHOUT GCS)
docker compose run --rm tests composer tests-abs
docker compose run --rm tests composer tests-s3
docker compose run --rm tests composer tests-gcs
docker compose run --rm tests composer tests-cross-backends
```

Integration tests require live Keboola credentials (S3/ABS/GCS access + project token).

## Related repositories

- Used in: `app-project-restore`
- Paired with: `php-kbc-project-backup`
