# php-kbc-project-restore – how the restore works

## Initializing the Restore class

```php
new S3Restore($sapiClient, $logger)  // or AbsRestore / GcsRestore
```

`Restore::__construct()` constructor:
1. Stores `sapiClient` and initializes `Token` (permission validation wrapper)
2. Fetches branch list (`DevBranches::listBranches()`)
3. Finds the default branch (`isDefault === true`)
4. Creates `BranchAwareClient` for operations on the default branch

## Complete restore flow

```
restoreProjectMetadata()
  └─ getDataFromStorage('defaultBranchMetadata.json')
  └─ DevBranchesMetadata::addBranchMetadata()

restoreBuckets(checkBackend)
  └─ getDataFromStorage('buckets.json')
  └─ forEach bucket: createBucket() + postBucketMetadata()

restoreConfigs(skipComponents)
  └─ getDataFromStorage('configurations.json')
  └─ forEach component (excluding skipComponents):
       forEach config: addConfiguration() + updateConfiguration()
       forEach row: addConfigurationRow() + updateConfigurationRow()
       addConfigurationMetadata()

restoreTables(parallelism)
  ├─ Phase 1: prepare workItems from 'tables.json'
  ├─ Phase 2: createTablesParallel (worker-create-table.php)
  └─ Phase 3: sequential data upload + column metadata

restoreTableAliases()
  └─ from 'tables.json', only where isAlias === true
  └─ createAliasTable()

restoreTriggers()
  └─ getDataFromStorage('triggers.json') → createTrigger()

restoreNotifications()
  └─ getDataFromStorage('notifications.json') → SubscriptionClient::createSubscription()

restorePermanentFiles()
  └─ getDataFromStorage('permanentFiles.json') → forEach: uploadFile()
```

## restoreConfigs – detail

```
getDataFromStorage('configurations.json')  →  list of components with configurations

forEach component:
  if componentId in skipComponents: skip + warn

  if componentId not in indexAction(): skip + warn

  forEach configuration:
    getDataFromStorage('configurations/<componentId>/<configId>.json')

    addConfiguration(id, name, description)
      if keboola.orchestrator: isDisabled = true

    ConfigurationCorrector::correct()
      → translates stack-dependent component IDs (StackSpecificComponentIdTranslator)
    updateConfiguration(configuration, state)

    forEach row:
      addConfigurationRow(rowId)
      updateConfigurationRow(configuration, state, isDisabled)

    if rowsSortOrder:
      updateConfiguration(rowsSortOrder)

    if .json.metadata exists:
      addConfigurationMetadata()
```

### Why orchestrator gets isDisabled = true

`keboola.orchestrator` configurations are restored structurally (flow, triggers, steps are preserved), but automatically set to `isDisabled: true`. Otherwise, orchestrations could start running immediately after restore, before data and configurations in the destination project are verified. Users must manually re-enable them.

## restoreTables – 3 phases

### Phase 1: Prepare work items

```
getDataFromStorage('tables.json')  →  list of all tables

forEach table:
  skip: isAlias === true
  skip: bucket was not restored
  checkTableRestorable(tableInfo)  – compatibility check

  workerInput = {
    sapiUrl, sapiToken, runId,
    bucketId, tableName, displayName,
    columns, primaryKey, isTyped,
    tableDefinition (for typed tables)
  }
```

### buildTypedTableDefinition

For typed tables (Snowflake native typing, BigQuery), `tableDefinition` is assembled with columns and their data types. Backend is read from `restoredBuckets` (column map `bucketId → backend`).

Special case BigQuery: Snowflake `NUMERIC` with `scale ≤ 9` maps to BigQuery `NUMERIC` instead of `BIGNUMERIC`.

### Phase 2: Parallel table creation

```
interleaveByBucket(workItems)
  → interleaves tables across buckets
  → [bucket_A_table_1, bucket_B_table_1, bucket_C_table_1, bucket_A_table_2, ...]
  → reason: Snowflake locks schema metadata, multiple tables in the same bucket would wait

createTablesParallel(workItems, parallelism):
  pendingItems = workItems
  runningProcesses = {}

  while pendingItems or runningProcesses:
    while free slots and pendingItems:
      process = createWorkerProcess(workerInput)
      process.start()
      runningProcesses[tableId] = process

    forEach completed process:
      output = json_decode(process.getOutput())

      if error:
        isNullablePkError → StorageApiException (PK on nullable column)
        isClientException → ClientException (4xx)
        else              → RuntimeException

      else:
        createdTableIds[originalTableId] = output.tableId

    usleep(100ms)  ← polling interval

  return createdTableIds
```

### Worker: worker-create-table.php

Child process. Receives JSON from STDIN, creates one table via SAPI, returns JSON on STDOUT.

**Input:**
```json
{
  "sapiUrl": "...", "sapiToken": "...", "runId": "...",
  "bucketId": "in.c-bucket", "tableName": "my_table",
  "displayName": "My Table", "columns": [...],
  "primaryKey": [...], "isTyped": true,
  "tableDefinition": {...}
}
```

**Output:**
```json
{
  "tableId": "in.c-bucket.my_table",
  "error": null, "code": null,
  "isNullablePkError": false,
  "isClientException": false
}
```

### Phase 3: Data and column metadata upload

```
forEach createdTable:
  restoreTableColumnsMetadata(tableInfo, tableId)
    → postColumnMetadata() for each column with metadata

  slices = listTableFiles(tableId)
    → iterates storage, finds table files

  if no files: skip (empty table)

  if 1 file and not .part_0.csv.gz (non-sliced):
    copyFileFromStorage(slice, localPath)
    uploadFile() → writeTableAsyncDirect(dataFileId)

  if sliced:
    forEach slice:
      copyFileFromStorage(slice, localPath)
    uploadSlicedFile(federationToken=true) → writeTableAsyncDirect(dataFileId, columns)
```

## restoreTableAliases – why after tables

An alias table is defined by a reference to the source table (`sourceTable.id`). If the alias were created before the source table exists, SAPI would return an error. Therefore `restoreTableAliases()` is always called after `restoreTables()`.

```
forEach table where isAlias === true:
  if not tableExists(sourceTableId): skip + warn
  if bucket does not exist: skip + warn
  createAliasTable(bucketId, sourceTableId, name, aliasFilter?, aliasColumns?)
  restoreTableColumnsMetadata()
```

## getDataFromStorage – implementation per backend

Abstract method `getDataFromStorage(string $name)` is implemented in each backend:

| Backend | Implementation |
|---------|-------------|
| `S3Restore` | `S3Client::getObject(Bucket, Key)` |
| `AbsRestore` | `BlobRestProxy::getBlob(container, name)` |
| `GcsRestore` | `StorageObject::downloadAsString()` |

Returns file content as string or stream.

## Dry-run mode

`setDryRunMode(true)`:
- All storage read operations proceed normally
- No SAPI write operations are executed (`addConfiguration`, `createBucket`, `uploadFile`, ...)
- Each skipped operation is logged as `[dry-run] ...`
