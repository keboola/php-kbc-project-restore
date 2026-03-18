<?php

declare(strict_types=1);

namespace Keboola\ProjectRestore;

use Closure;
use Composer\Autoload\ClassLoader;
use Keboola\Csv\CsvFile;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\NotificationClient\Requests\PostSubscription\EmailRecipient;
use Keboola\NotificationClient\Requests\PostSubscription\Filter;
use Keboola\NotificationClient\Requests\PostSubscription\FilterOperator;
use Keboola\NotificationClient\Requests\Subscription;
use Keboola\NotificationClient\SubscriptionClient;
use Keboola\ProjectRestore\StorageApi\BucketInfo;
use Keboola\ProjectRestore\StorageApi\ConfigurationCorrector;
use Keboola\ProjectRestore\StorageApi\Token;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\DevBranchesMetadata;
use Keboola\StorageApi\Exception as StorageApiException;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ConfigurationMetadata;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Keboola\StorageApi\Options\Components\ConfigurationRowState;
use Keboola\StorageApi\Options\Components\ConfigurationState;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\Metadata\TableMetadataUpdateOptions;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\StorageApi\Tokens;
use Keboola\Temp\Temp;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use ReflectionClass;
use RuntimeException;
use stdClass;
use Symfony\Component\Process\Process;
use Throwable;

abstract class Restore
{
    protected Client $sapiClient;

    protected BranchAwareClient $branchAwareClient;

    protected LoggerInterface $logger;

    protected Token $token;

    private const ORCHESTRATOR_COMPONENT_ID = 'keboola.orchestrator';

    private const METADATA_BATCH_SIZE = 50;

    protected bool $dryRun = false;

    private bool $forcePrimaryKeyNotNull = false;

    private int $parallelism = 5;

    /** @var Closure(array<string, mixed>): Process|null */
    private ?Closure $workerProcessFactory = null;

    public function __construct(Client $sapiClient, ?LoggerInterface $logger = null)
    {
        $this->sapiClient = $sapiClient;
        $this->token = new Token($this->sapiClient);
        $this->logger = $logger ?: new NullLogger();

        $devBranches = new DevBranches($this->sapiClient);
        $listBranches = $devBranches->listBranches();
        $defaultBranch = current(array_filter($listBranches, fn($v) => $v['isDefault'] === true));

        $this->branchAwareClient = new BranchAwareClient(
            $defaultBranch['id'],
            [
                'url' => $sapiClient->getApiUrl(),
                'token' => $sapiClient->getTokenString(),
            ],
        );
    }

    public function setDryRunMode(bool $dryRun = true): void
    {
        $this->dryRun = $dryRun;
    }

    public function setForcePrimaryKeyNotNull(bool $force = true): void
    {
        $this->forcePrimaryKeyNotNull = $force;
    }

    public function setParallelism(int $parallelism): void
    {
        $this->parallelism = $parallelism;
    }

    /** @param Closure(array<string, mixed>): Process $factory */
    public function setWorkerProcessFactory(Closure $factory): void
    {
        $this->workerProcessFactory = $factory;
    }

    public function restoreProjectMetadata(): void
    {
        $devBranchMetadata = new DevBranchesMetadata($this->branchAwareClient);

        $fileContent = $this->getDataFromStorage('defaultBranchMetadata.json');
        /** @var array $projectMetadata */
        $projectMetadata = json_decode((string) $fileContent, true);

        if ($projectMetadata) {
            if ($this->dryRun === false) {
                $devBranchMetadata->addBranchMetadata($projectMetadata);
            } else {
                $this->logger->info('[dry-run] Restore project metadata');
            }
        }
    }

    public function restoreConfigs(array $skipComponents = []): void
    {
        $this->logger->info('Downloading configurations');

        $fileContent = $this->getDataFromStorage('configurations.json');
        /** @var array $configurations */
        $configurations = json_decode((string) $fileContent, true);

        $components = new Components($this->sapiClient);
        $verifyToken = $this->sapiClient->verifyToken();

        $componentList = [];
        foreach ($this->sapiClient->indexAction()['components'] as $component) {
            $componentList[$component['id']] = $component;
        }

        /** @var array $componentWithConfigurations */
        foreach ($configurations as $componentWithConfigurations) {
            $componentId = $componentWithConfigurations['id'];

            if (in_array($componentId, $skipComponents, true)) {
                $this->logger->warning(
                    sprintf(
                        'Skipping %s configurations - component marked as skipped',
                        $componentId,
                    ),
                );
                continue;
            }

            // skip non-existing components
            if (!array_key_exists($componentId, $componentList)) {
                $this->logger->warning(
                    sprintf(
                        'Skipping %s configurations - component does not exists',
                        $componentId,
                    ),
                );
                continue;
            }

            $this->logger->info(sprintf('Restoring %s configurations', $componentId));

            // restore configuration metadata
            $componentConfigurationsFiles = $this->listComponentConfigurationsFiles(sprintf(
                'configurations/%s',
                $componentId,
            ));

            /** @var array $componentConfiguration */
            foreach ($componentWithConfigurations['configurations'] as $componentConfiguration) {
                // configurations as objects to preserve empty arrays or empty objects
                /** @var stdClass $configurationData */
                $configurationData = json_decode((string) $this->getDataFromStorage(sprintf(
                    'configurations/%s/%s.json',
                    $componentId,
                    $componentConfiguration['id'],
                )));

                // create empty configuration
                $configuration = new Configuration();
                $configuration->setComponentId($componentId);
                $configuration->setConfigurationId($componentConfiguration['id']);
                $configuration->setDescription($configurationData->description);
                $configuration->setName($configurationData->name);
                if ($componentId === self::ORCHESTRATOR_COMPONENT_ID) {
                    $configuration->setIsDisabled(true);
                }

                if ($this->dryRun === false) {
                    $components->addConfiguration($configuration);
                } else {
                    $this->logger->info(sprintf(
                        '[dry-run] Create configuration %s (component "%s")',
                        $componentConfiguration['id'],
                        $componentId,
                    ));
                }

                // update configuration and state
                $configuration->setChangeDescription(sprintf(
                    'Configuration %s restored from backup',
                    $componentConfiguration['id'],
                ));

                $configCorrector = new ConfigurationCorrector($this->sapiClient->getApiUrl(), $this->logger);
                $configuration->setConfiguration(
                    $configCorrector->correct(
                        $componentId,
                        $configurationData->configuration,
                        $verifyToken['owner']['defaultBackend'],
                    ),
                );

                if ($this->dryRun === false) {
                    $components->updateConfiguration($configuration);
                } else {
                    $this->logger->info(sprintf(
                        '[dry-run] Update configuration %s (component "%s")',
                        $componentConfiguration['id'],
                        $componentId,
                    ));
                }

                if (isset($configurationData->state)) {
                    $configurationState = new ConfigurationState();
                    $configurationState->setComponentId($componentId);
                    $configurationState->setConfigurationId($componentConfiguration['id']);
                    $configurationState->setState($configurationData->state);

                    if ($this->dryRun === false) {
                        $components->updateConfigurationState($configurationState);
                    } else {
                        $this->logger->info(sprintf(
                            '[dry-run] Restore state of configuration %s (component "%s")',
                            $componentConfiguration['id'],
                            $componentId,
                        ));
                    }
                }

                // create configuration rows
                if (count($configurationData->rows)) {
                    foreach ($configurationData->rows as $row) {
                        // create empty row
                        $configurationRow = new ConfigurationRow($configuration);
                        $configurationRow->setRowId($row->id);
                        if ($this->dryRun === false) {
                            $components->addConfigurationRow($configurationRow);
                        } else {
                            $this->logger->info(sprintf(
                                '[dry-run] Create configuration row %s (configuration %s, component "%s")',
                                $row->id,
                                $componentConfiguration['id'],
                                $componentId,
                            ));
                        }

                        // update row configuration and state
                        $configurationRow->setConfiguration($row->configuration);
                        $configurationRow->setChangeDescription(sprintf('Row %s restored from backup', $row->id));
                        $configurationRow->setName($row->name);
                        $configurationRow->setDescription($row->description);
                        $configurationRow->setIsDisabled($row->isDisabled);

                        if ($this->dryRun === false) {
                            $components->updateConfigurationRow($configurationRow);
                        } else {
                            $this->logger->info(sprintf(
                                '[dry-run] Update row %s of configuration %s (component "%s")',
                                $row->id,
                                $componentConfiguration['id'],
                                $componentId,
                            ));
                        }

                        if (isset($row->state)) {
                            $configurationRowState = new ConfigurationRowState($configuration);
                            $configurationRowState->setRowId($configurationRow->getRowId());
                            $configurationRowState->setState($row->state);

                            if ($this->dryRun === false) {
                                $components->updateConfigurationRowState($configurationRowState);
                            } else {
                                $this->logger->info(sprintf(
                                    '[dry-run] Restore state of configuration row %s'
                                    . ' (configuration %s, component "%s")',
                                    // @phpstan-ignore-next-line
                                    $row->id,
                                    $componentConfiguration['id'],
                                    $componentId,
                                ));
                            }
                        }
                    }
                }

                // restore row sorting
                if (!empty($configurationData->rowsSortOrder)) {
                    $configuration->setRowsSortOrder($configurationData->rowsSortOrder);
                    $configuration->setChangeDescription('Restored rows sort order from backup');

                    if ($this->dryRun === false) {
                        $components->updateConfiguration($configuration);
                    } else {
                        $this->logger->info(sprintf(
                            '[dry-run] Restore rows sort order (configuration %s, component "%s")',
                            $componentConfiguration['id'],
                            $componentId,
                        ));
                    }
                }

                // restore configuration metadata
                $metadataFilePath = sprintf(
                    'configurations/%s/%s.json.metadata',
                    $componentId,
                    $componentConfiguration['id'],
                );

                if (in_array($metadataFilePath, $componentConfigurationsFiles, true)) {
                    /** @var array $metadataData */
                    $metadataData = json_decode((string) $this->getDataFromStorage($metadataFilePath), true);
                    array_walk($metadataData, function (&$v): void {
                        unset($v['id']);
                        unset($v['timestamp']);
                    });

                    $branchAwareComponents = new Components($this->branchAwareClient);

                    $configMetadata = new ConfigurationMetadata($configuration);
                    $configMetadata->setMetadata($metadataData);

                    if ($this->dryRun === false) {
                        $branchAwareComponents->addConfigurationMetadata($configMetadata);
                    } else {
                        $this->logger->info(sprintf(
                            '[dry-run] Restore metadata of configuration %s (component "%s")',
                            $componentConfiguration['id'],
                            $componentId,
                        ));
                    }
                }
            }
        }
    }

    public function getBucketsInBackup(): array
    {
        $this->logger->info('Downloading buckets');

        $fileContent = $this->getDataFromStorage('buckets.json');

        /** @var array $buckets */
        $buckets = json_decode((string) $fileContent, true);

        return array_map(function (array $bucketInfo) {
            return new BucketInfo($bucketInfo);
        }, $buckets);
    }

    public function restoreBucket(BucketInfo $bucket, bool $useDefaultBackend = false): bool
    {
        if ($bucket->isLinkedBucket()) {
            throw new StorageApiException('Linked bucket restore is not supported');
        }

        if (substr($bucket->getName(), 0, 2) !== 'c-') {
            throw new StorageApiException('System bucket restore is not supported');
        }

        $this->logger->info(sprintf('Restoring bucket %s', $bucket->getId()));

        if ($this->dryRun === false) {
            $this->sapiClient->createBucket(
                substr($bucket->getName(), 2),
                $bucket->getStage(),
                $bucket->getDescription() ?: '',
                $useDefaultBackend ? null : $bucket->getBackend(),
                $bucket->getDisplayName(),
            );
        } else {
            $this->logger->info(sprintf(
                '[dry-run] Restore bucket "%s/%s"',
                $bucket->getStage(),
                $bucket->getName(),
            ));
        }

        // bucket metadata
        if (count($bucket->getMetadata())) {
            $metadataClient = new Metadata($this->sapiClient);
            foreach ($this->prepareMetadata($bucket->getMetadata()) as $provider => $metadata) {
                if ($this->dryRun === false) {
                    $metadataClient->postBucketMetadata($bucket->getId(), (string) $provider, $metadata);
                } else {
                    $this->logger->info(sprintf(
                        '[dry-run] Restore metadata of bucket "%s/%s" (provider "%s")',
                        $bucket->getStage(),
                        $bucket->getName(),
                        $provider,
                    ));
                }
            }
        }

        return true;
    }

    public function restoreBuckets(bool $checkBackend = true): void
    {
        $buckets = $this->getBucketsInBackup();

        if ($checkBackend) {
            foreach ($buckets as $bucketInfo) {
                switch ($bucketInfo->getBackend()) {
                    case 'mysql':
                        if (!$this->token->hasProjectMysqlBackend()) {
                            throw new StorageApiException('Missing MySQL backend');
                        }
                        break;
                    case 'redshift':
                        if (!$this->token->hasProjectRedshiftBackend()) {
                            throw new StorageApiException('Missing Redshift backend');
                        }
                        break;
                    case 'snowflake':
                        if (!$this->token->hasProjectSnowflakeBackend()) {
                            throw new StorageApiException('Missing Snowflake backend');
                        }
                        break;
                }
            }
        }

        $metadataClient = new Metadata($this->sapiClient);

        // buckets restore
        foreach ($buckets as $bucketInfo) {
            try {
                $this->restoreBucket($bucketInfo, !$checkBackend);
            } catch (StorageApiException $e) {
                $this->logger->warning(sprintf('Skipping bucket %s - %s', $bucketInfo->getId(), $e->getMessage()));
                continue;
            }

            if (count($bucketInfo->getMetadata())) {
                foreach ($this->prepareMetadata($bucketInfo->getMetadata()) as $provider => $metadata) {
                    if ($this->dryRun === false) {
                        $metadataClient->postBucketMetadata($bucketInfo->getId(), (string) $provider, $metadata);
                    } else {
                        $this->logger->info(sprintf(
                            '[dry-run] Restore metadata of bucket "%s/%s" (provider "%s")',
                            $bucketInfo->getStage(),
                            $bucketInfo->getName(),
                            $provider,
                        ));
                    }
                }
            }
        }
    }

    public function restoreTableAliases(): void
    {
        $this->logger->info('Downloading tables');

        $fileContent = $this->getDataFromStorage('tables.json');
        /** @var array $tables */
        $tables = json_decode((string) $fileContent, true);

        $restoredBuckets = array_map(
            function ($bucket) {
                return $bucket['id'];
            },
            $this->sapiClient->listBuckets(),
        );
        $metadataClient = new Metadata($this->sapiClient);

        /** @var array $tableInfo */
        foreach ($tables as $tableInfo) {
            if ($tableInfo['isAlias'] !== true) {
                continue;
            }

            $bucketId = $tableInfo['bucket']['id'];
            /** @var string $tableId */
            $tableId = $tableInfo['id'];
            $sourceTableId = $tableInfo['sourceTable']['id'];

            if (!$this->sapiClient->tableExists($sourceTableId)) {
                $this->logger->warning(sprintf(
                    'Skipping alias %s - source table with id "%s" does not exist',
                    $tableId,
                    $sourceTableId,
                ));
                continue;
            }

            if (!in_array($bucketId, $restoredBuckets)) {
                $this->logger->warning(sprintf('Skipping alias %s', $tableId));
                continue;
            }

            $this->logger->info(sprintf('Restoring alias %s', $tableId));

            $aliasOptions = [];
            if (isset($tableInfo['aliasFilter'])) {
                $aliasOptions['aliasFilter'] = $tableInfo['aliasFilter'];
            }
            if (isset($tableInfo['aliasColumnsAutoSync']) && $tableInfo['aliasColumnsAutoSync'] === false) {
                $aliasOptions['aliasColumns'] = $tableInfo['columns'];
            }

            if ($this->dryRun === false) {
                $this->sapiClient->createAliasTable(
                    $bucketId,
                    $tableInfo['sourceTable']['id'],
                    $tableInfo['name'],
                    $aliasOptions,
                );

                $this->restoreTableColumnsMetadata($tableInfo, $tableId, $metadataClient);
            } else {
                $this->logger->info(sprintf('[dry-run] Restore alias %s', $tableId));
            }
        }
    }

    public function restoreTables(): void
    {
        $this->logger->info('Downloading tables');

        $fileContent = $this->getDataFromStorage('tables.json');
        /** @var array $tables */
        $tables = json_decode((string) $fileContent, true);

        // carry the backend type to use for typed tables
        $restoredBuckets = array_column($this->sapiClient->listBuckets(), 'backend', 'id');

        $metadataClient = new Metadata($this->sapiClient);

        // Phase 1: prepare worker inputs (fast, no SAPI I/O)
        /** @var array<int, array{tableInfo: array<mixed>, workerInput: array<string, mixed>}> $workItems */
        $workItems = [];
        /** @var array $tableInfo */
        foreach ($tables as $tableInfo) {
            if ($tableInfo['isAlias'] === true) {
                continue;
            }
            $this->checkTableRestorable($tableInfo);

            $originalTableId = $tableInfo['id'];
            $bucketId = $tableInfo['bucket']['id'];

            if (!in_array($bucketId, array_keys($restoredBuckets))) {
                $this->logger->warning(sprintf('Skipping table %s', $originalTableId));
                continue;
            }

            $this->logger->info(sprintf('Restoring table %s', $originalTableId));

            if ($this->dryRun === true) {
                $this->logger->info(sprintf('[dry-run] Restore table %s', $originalTableId));
                continue;
            }

            $isTyped = $tableInfo['isTyped'] ?? false;
            /** @var array<string, mixed> $workerInput */
            $workerInput = [
                'sapiUrl' => $this->sapiClient->getApiUrl(),
                'sapiToken' => $this->sapiClient->getTokenString(),
                'isTyped' => $isTyped,
                'bucketId' => $bucketId,
                'tableName' => $tableInfo['name'],
                'columns' => $tableInfo['columns'],
                'primaryKey' => $tableInfo['primaryKey'],
            ];

            if (isset($tableInfo['displayName'])) {
                $workerInput['displayName'] = $tableInfo['displayName'];
            }

            if ($isTyped) {
                $workerInput['tableDefinition'] = $this->buildTypedTableDefinition(
                    $tableInfo,
                    $restoredBuckets[$bucketId],
                );
            }

            $workItems[] = [
                'tableInfo' => $tableInfo,
                'workerInput' => $workerInput,
            ];
        }

        if ($workItems === []) {
            return;
        }

        // Phase 2: parallel table creation
        $createdTableIds = $this->createTablesParallel($workItems);

        // Phase 3: sequential metadata + data upload
        $tmp = new Temp();
        foreach ($workItems as $workItem) {
            $tableInfo = $workItem['tableInfo'];
            $originalTableId = $tableInfo['id'];
            $tableId = $createdTableIds[$originalTableId] ?? null;
            if ($tableId === null) {
                continue;
            }

            $this->restoreTableColumnsMetadata($tableInfo, $tableId, $metadataClient);

            $slices = $this->listTableFiles($tableId);

            // no files for the table found, probably an empty table
            if (count($slices) === 0) {
                continue;
            }

            $headerFile = $tmp->createFile(sprintf('%s.header.csv', $tableInfo['id']));
            $headerFile = new CsvFile($headerFile->getPathname());
            $headerFile->writeRow($tableInfo['columns']);

            $firstSlice = reset($slices);
            if (count($slices) === 1 && substr($firstSlice, -14) !== '.part_0.csv.gz') {
                // one file and no slices => the file has header
                // no slices = file does not end with .part_0.csv.gz
                $fileName = sprintf('%s.csv.gz', $tableId);

                $targetFile = $tmp->createFile($fileName);

                $this->copyFileFromStorage($firstSlice, $targetFile->getPathname());

                $fileUploadOptions = new FileUploadOptions();
                $fileUploadOptions
                    ->setFileName(sprintf('%s.csv.gz', $tableId));
                $fileId = $this->sapiClient->uploadFile($targetFile->getPathname(), $fileUploadOptions);
                $this->sapiClient->writeTableAsyncDirect(
                    $tableId,
                    [
                        'name' => $tableInfo['name'],
                        'dataFileId' => $fileId,
                    ],
                );
            } else {
                $downloadedSlices = [];
                foreach ($slices as $part => $slice) {
                    $fileName = $tmp->getTmpFolder() . '/' . $tableId . $tableId . '.part_' . $part . '.csv.gz';
                    $downloadedSlices[] = $fileName;

                    $this->copyFileFromStorage($slice, $fileName);
                }

                $fileUploadOptions = new FileUploadOptions();
                $fileUploadOptions
                    ->setFederationToken(true)
                    ->setFileName($tableId)
                    ->setIsSliced(true)
                    ->setIsEncrypted(true);

                $dataFileId = $this->sapiClient->uploadSlicedFile($downloadedSlices, $fileUploadOptions);

                // Upload data to table
                $this->sapiClient->writeTableAsyncDirect(
                    $tableId,
                    [
                        'dataFileId' => $dataFileId,
                        'columns' => $headerFile->getHeader(),
                    ],
                );
            }
            unset($headerFile);
        }
    }

    /**
     * @param array<int, array{tableInfo: array<mixed>, workerInput: array<string, mixed>}> $workItems
     * @return array<string, string> map of originalTableId => createdTableId
     */
    private function createTablesParallel(array $workItems): array
    {
        $pendingItems = $workItems;
        /** @var array<string, array{process: Process, tableInfo: array<mixed>}> $runningProcesses */
        $runningProcesses = [];
        /** @var array<string, string> $createdTableIds */
        $createdTableIds = [];
        /** @var Throwable[] $errors */
        $errors = [];

        try {
            while ($pendingItems !== [] || $runningProcesses !== []) {
                while (count($runningProcesses) < $this->parallelism && $pendingItems !== []) {
                    $item = array_shift($pendingItems);
                    /** @var string $originalTableId */
                    $originalTableId = $item['tableInfo']['id'];
                    $process = $this->createWorkerProcess($item['workerInput']);
                    $process->start();
                    $runningProcesses[$originalTableId] = [
                        'process' => $process,
                        'tableInfo' => $item['tableInfo'],
                    ];
                }

                foreach ($runningProcesses as $originalTableId => $running) {
                    if ($running['process']->isRunning()) {
                        continue;
                    }
                    unset($runningProcesses[$originalTableId]);

                    /** @var array{tableId?: string, error?: string, code?: int, isNullablePkError?: bool, isClientException?: bool}|null $output */
                    $output = json_decode($running['process']->getOutput(), true);

                    if ($running['process']->getExitCode() !== 0 || isset($output['error'])) {
                        $errorMessage = ($output !== null && isset($output['error']))
                            ? (string) $output['error']
                            : $running['process']->getErrorOutput();

                        if ($output !== null && ($output['isNullablePkError'] ?? false)) {
                            /** @var array{name: string} $tableInfo */
                            $tableInfo = $running['tableInfo'];
                            $errors[] = new StorageApiException(sprintf(
                                'Table "%s" cannot be restored because the primary key cannot be'
                                . ' set on a nullable column.',
                                $tableInfo['name'],
                            ));
                        } elseif ($output !== null && ($output['isClientException'] ?? false)) {
                            $errors[] = new ClientException($errorMessage, (int) ($output['code'] ?? 0));
                        } else {
                            $errors[] = new RuntimeException(sprintf(
                                'Failed to create table %s: %s',
                                $originalTableId,
                                $errorMessage,
                            ));
                        }
                        continue;
                    }

                    $createdTableIds[$originalTableId] = (string) ($output['tableId'] ?? $originalTableId);
                }

                if ($runningProcesses !== []) {
                    usleep(100000);
                }
            }
        } finally {
            foreach ($runningProcesses as $running) {
                if ($running['process']->isRunning()) {
                    $running['process']->stop();
                }
            }
        }

        if ($errors !== []) {
            throw $errors[0];
        }

        return $createdTableIds;
    }

    /** @param array<string, mixed> $input */
    private function createWorkerProcess(array $input): Process
    {
        if ($this->workerProcessFactory !== null) {
            return ($this->workerProcessFactory)($input);
        }

        $classLoaderFile = (new ReflectionClass(ClassLoader::class))->getFileName();
        $input['autoloadPath'] = dirname((string) $classLoaderFile, 2) . '/autoload.php';

        $process = new Process([PHP_BINARY, __DIR__ . '/worker-create-table.php']);
        $process->setInput((string) json_encode($input));
        $process->setTimeout(3600);
        return $process;
    }

    /**
     * @return array<string|int, array<int, array{key: string, value: string, columnName?: string}>>
     */
    private function prepareMetadata(array $rawMetadata): array
    {
        $result = [];
        foreach ($rawMetadata as $item) {
            $result[$item['provider']][] = [
                'key' => $item['key'],
                'value' => $item['value'],
            ];
        }
        return $result;
    }

    public function listConfigsInBackup(string $componentId): array
    {
        $this->logger->info('Downloading configurations');

        $fileContent = $this->getDataFromStorage('configurations.json');

        /** @var array $components */
        $components = json_decode((string) $fileContent, true);
        /** @var array $component */
        foreach ($components as $component) {
            if ($component['id'] !== $componentId) {
                continue;
            }

            return array_map(
                function (array $configuration) {
                    return $configuration['id'];
                },
                $component['configurations'],
            );
        }

        return [];
    }

    /**
     * @return resource|string
     */
    abstract protected function getDataFromStorage(string $filePath, bool $useString = true);

    abstract protected function listComponentConfigurationsFiles(string $filePath): array;

    abstract protected function copyFileFromStorage(string $sourceFilePath, string $targetFilePath): void;

    abstract protected function listTableFiles(string $tableId): array;

    /** @return array<string, mixed> */
    private function buildTypedTableDefinition(array $tableInfo, string $destinationBucketBackendType): array
    {
        $columns = [];
        foreach ($tableInfo['columns'] as $column) {
            $columns[$column] = [];
        }
        foreach ($tableInfo['columnMetadata'] ?? [] as $columnName => $column) {
            $columnName = strval($columnName);
            $columnMetadata = [];
            foreach ($column as $metadata) {
                if ($metadata['provider'] !== 'storage') {
                    continue;
                }
                $columnMetadata[$metadata['key']] = $metadata['value'];
            }
            if ($destinationBucketBackendType !== $tableInfo['bucket']['backend']) {
                $this->validateSnowflakeToBigqueryNumericScale(
                    $tableInfo['bucket']['backend'],
                    $destinationBucketBackendType,
                    $columnName,
                    $columnMetadata,
                );
                $sourceBaseType = $this->getBaseType(
                    $tableInfo['bucket']['backend'],
                    $columnMetadata['KBC.datatype.type'],
                );
                switch ($destinationBucketBackendType) {
                    case 'snowflake':
                        $definition = (Snowflake::getDefinitionForBasetype((string) $sourceBaseType))->toArray();
                        break;
                    case 'bigquery':
                        $definition = (Bigquery::getDefinitionForBasetype((string) $sourceBaseType))->toArray();
                        break;
                    default:
                        $this->logger->warning('unsupported typed backend type');
                        continue 2;
                }
                // the nullable property is required for PK compatability
                $definition['nullable'] = $columnMetadata['KBC.datatype.nullable'] === '1';
            } else {
                $definition = [
                    'type' => $columnMetadata['KBC.datatype.type'],
                    'nullable' => $columnMetadata['KBC.datatype.nullable'] === '1',
                ];
                if (isset($columnMetadata['KBC.datatype.length'])) {
                    $definition['length'] = $columnMetadata['KBC.datatype.length'];
                }
                if (isset($columnMetadata['KBC.datatype.default'])) {
                    $definition['default'] = $columnMetadata['KBC.datatype.default'];
                }
            }
            if ($this->forcePrimaryKeyNotNull
                && in_array($columnName, $tableInfo['primaryKey'], true)
                && $definition['nullable'] === true
            ) {
                $this->logger->warning(sprintf(
                    'Table "%s": primary key column "%s" is nullable in source, forcing NOT NULL in destination.',
                    $tableInfo['name'],
                    $columnName,
                ));
                $definition['nullable'] = false;
            }
            $columns[$columnName] = [
                'name' => $columnName,
                'definition' => $definition,
                'basetype' => $columnMetadata['KBC.datatype.basetype'],
            ];
        }

        $data = [
            'name' => $tableInfo['name'],
            'primaryKeysNames' => $tableInfo['primaryKey'],
            'columns' => array_values($columns),
        ];

        if ($tableInfo['bucket']['backend'] === 'synapse') {
            $data['distribution'] = [
                'type' => $tableInfo['distributionType'],
                'distributionColumnsNames' => $tableInfo['distributionKey'],
            ];
            $data['index'] = [
                'type' => $tableInfo['indexType'],
                'indexColumnsNames' => $tableInfo['indexKey'],
            ];
        }

        return $data;
    }

    private function validateSnowflakeToBigqueryNumericScale(
        string $sourceBackend,
        string $destinationBackend,
        string $columnName,
        array $columnMetadata,
    ): void {
        if ($sourceBackend !== 'snowflake' || $destinationBackend !== 'bigquery') {
            return;
        }
        if (strtoupper((string) ($columnMetadata['KBC.datatype.basetype'] ?? '')) !== 'NUMERIC') {
            return;
        }
        $length = $columnMetadata['KBC.datatype.length'] ?? null;
        if ($length === null) {
            return;
        }
        $parts = explode(',', $length);
        if (count($parts) !== 2) {
            return;
        }
        $scale = (int) trim($parts[1]);
        if ($scale > 9) {
            throw new StorageApiException(sprintf(
                'Column "%s" has type NUMBER(%s) which exceeds BigQuery\'s maximum scale of 9. '
                . 'BigQuery supports NUMERIC with scale up to 9. '
                . 'Please adjust the column type before restoring.',
                $columnName,
                $length,
            ));
        }
    }

    private function getBaseType(string $backend, string $originalType): ?string
    {
        switch ($backend) {
            case 'snowflake':
                return (new Snowflake($originalType))->getBasetype();
            case 'bigquery':
                return (new Bigquery($originalType))->getBasetype();
            default:
                return null;
        }
    }

    private function restoreTableColumnsMetadata(array $tableInfo, string $tableId, Metadata $metadataClient): void
    {
        $metadatas = [];
        if (isset($tableInfo['metadata']) && count($tableInfo['metadata'])) {
            foreach ($this->prepareMetadata($tableInfo['metadata']) as $provider => $metadata) {
                $metadatas[$provider]['table'] = $metadata;
            }
        }
        if (isset($tableInfo['columnMetadata']) && count($tableInfo['columnMetadata'])) {
            foreach ($tableInfo['columnMetadata'] as $column => $columnMetadata) {
                foreach ($this->prepareMetadata($columnMetadata) as $provider => $metadata) {
                    if ($metadata !== []) {
                        $metadatas[$provider]['columns'][$column] = $metadata;
                    }
                }
            }
        }

        /** @var array $metadata */
        foreach ($metadatas as $provider => $metadata) {
            if ($provider === 'storage') {
                continue;
            }

            // Split column metadata into batches to avoid AWS SNS message size limits
            if (isset($metadata['columns']) && count($metadata['columns']) > self::METADATA_BATCH_SIZE) {
                $columnChunks = array_chunk($metadata['columns'], self::METADATA_BATCH_SIZE, true);
                $totalBatches = count($columnChunks);

                $this->logger->info(sprintf(
                    'Processing table %s metadata in %d batches (%d columns per batch)',
                    $tableId,
                    $totalBatches,
                    self::METADATA_BATCH_SIZE,
                ));

                foreach ($columnChunks as $batchIndex => $columnChunk) {
                    $batchNumber = $batchIndex + 1;

                    // Include table metadata only in the first batch
                    $tableMetadataUpdateOptions = new TableMetadataUpdateOptions(
                        $tableId,
                        (string) $provider,
                        $batchIndex === 0 ? ($metadata['table'] ?? null) : null,
                        $columnChunk,
                    );

                    $metadataClient->postTableMetadataWithColumns($tableMetadataUpdateOptions);
                    $this->logger->info(sprintf(
                        'Processed batch %d/%d for table %s',
                        $batchNumber,
                        $totalBatches,
                        $tableId,
                    ));
                }
            } else {
                // Small number of columns, process in single batch
                $tableMetadataUpdateOptions = new TableMetadataUpdateOptions(
                    $tableId,
                    (string) $provider,
                    $metadata['table'] ?? null,
                    $metadata['columns'] ?? null,
                );

                $metadataClient->postTableMetadataWithColumns($tableMetadataUpdateOptions);
            }
        }
    }

    public function restorePermanentFiles(): void
    {
        $this->logger->info('Downloading permanent files');
        $fileContent = $this->getDataFromStorage('permanentFiles.json');

        /** @var array $permanentFiles */
        $permanentFiles = json_decode((string) $fileContent, true);

        $tmp = new Temp();

        if ($this->dryRun === true) {
            $this->logger->info(sprintf('[dry-run] Restoring %s permanent files', count($permanentFiles)));
            // skip all code bellow in dry-run mode
            return;
        }
        /** @var array $permanentFile */
        foreach ($permanentFiles as $permanentFile) {
            $permanentFileId = $permanentFile['id'];
            $permanentFileName = $permanentFile['name'];
            $this->logger->info(sprintf('Restoring file %s (ID: %s)', $permanentFileName, $permanentFileId));

            $fileName = $tmp->createFile($permanentFileName)->getPathname();
            $this->copyFileFromStorage('files/' . $permanentFileId, $fileName);

            $fileOption = new FileUploadOptions();
            $fileOption->setIsPermanent(true);
            $fileOption->setTags($permanentFile['tags'] ?? []);
            $this->sapiClient->uploadFile($fileName, $fileOption);
        }
    }

    public function restoreTriggers(): void
    {
        $this->logger->info('Downloading triggers');
        $fileContent = $this->getDataFromStorage('triggers.json');

        $triggers = (array) json_decode((string) $fileContent, true);

        if ($this->dryRun === true) {
            $this->logger->info(sprintf('[dry-run] Restoring %s triggers', count($triggers)));
            // skip all code bellow in dry-run mode
            return;
        }
        $actualToken = new Token($this->sapiClient);

        $tokensEndpoint = new Tokens($this->sapiClient);

        /**
         * @var array{
         *     id: string,
         *     configurationId: string,
         *     name: string,
         *     tables: array{tableId: string}[],
         * } $trigger
         */
        foreach ($triggers as $trigger) {
            if (!$trigger['tables']) {
                $this->logger->info(sprintf('Skipping trigger "%s" - no tables', $trigger['id']));
                continue;
            }
            $this->logger->info(sprintf('Restoring trigger "%s"', $trigger['id']));
            $tokenOptions = new TokenCreateOptions();
            $tokenOptions->setDescription(sprintf(
                '[_internal] Token for triggering %s',
                $trigger['configurationId'],
            ));
            $tokenOptions->setCanManageBuckets(true);
            $tokenOptions->setCanReadAllFileUploads(true);

            $token = $tokensEndpoint->createToken($tokenOptions);
            $trigger['runWithTokenId'] = $token['id'];
            $trigger['creatorToken'] = [
                'id' => $actualToken->getId(),
                'description' => $actualToken->getDescription(),
            ];
            $trigger['tableIds'] = array_map(fn($v) => $v['tableId'], $trigger['tables']);

            unset($trigger['tables']);

            try {
                $this->sapiClient->createTrigger($trigger);
            } catch (ClientException $e) {
                $this->logger->warning(sprintf(
                    'Trigger cannot be restored: %s',
                    $e->getMessage(),
                ));
            }
        }
    }

    public function restoreNotifications(): void
    {
        $this->logger->info('Downloading notifications');
        $fileContent = $this->getDataFromStorage('notifications.json');

        $notifications = (array) json_decode((string) $fileContent, true);

        if ($this->dryRun === true) {
            $this->logger->info(sprintf('[dry-run] Restoring %s notifications', count($notifications)));
            // skip all code bellow in dry-run mode
            return;
        }

        $subscriptionClient = new SubscriptionClient(
            $this->sapiClient->getServiceUrl('notification'),
            $this->sapiClient->getTokenString(),
            [
                'backoffMaxTries' => 3,
                'userAgent' => 'Keboola Project Restore',
            ],
        );

        /**
         * @var array{
         *     id: string,
         *     event: string,
         *     recipient: array{address: string},
         *     filters: array{field: string, operator: string, value: string}[],
         * } $notification
         */
        foreach ($notifications as $notification) {
            $this->logger->info(sprintf('Restoring notification %s', $notification['id']));

            $filters = [];
            foreach ($notification['filters'] as $filter) {
                if ($filter['field'] === 'branch.id') {
                    $filter['value'] = $this->branchAwareClient->getCurrentBranchId();
                }
                $filters[] = new Filter(
                    (string) $filter['field'],
                    (string) $filter['value'],
                    new FilterOperator($filter['operator']),
                );
            }

            $subscriptionClient->createSubscription(new Subscription(
                $notification['event'],
                new EmailRecipient($notification['recipient']['address']),
                $filters,
            ));
        }
    }

    private function checkTableRestorable(array $tableInfo): void
    {
        // check if primage key has nullable column
        if (($tableInfo['isTyped'] ?? false) === true) {
            foreach ($tableInfo['columnMetadata'] as $columnName => $columnMetadata) {
                if (!in_array($columnName, $tableInfo['primaryKey'], true)) {
                    continue;
                }
                $columnMetadata = array_filter(
                    $columnMetadata,
                    fn($v) => $v['provider'] === 'storage',
                );

                $columnMetadataList = array_combine(
                    array_map(fn($v) => $v['key'], $columnMetadata),
                    array_map(fn($v) => $v['value'], $columnMetadata),
                );
                if (isset($columnMetadataList['KBC.datatype.nullable']) &&
                    $columnMetadataList['KBC.datatype.nullable'] === '1') {
                    if ($this->forcePrimaryKeyNotNull) {
                        $this->logger->info(sprintf(
                            'Table "%s": primary key column "%s" is nullable, will be forced to NOT NULL.',
                            $tableInfo['name'],
                            $columnName,
                        ));
                    } else {
                        $this->logger->warning(sprintf(
                            'Table "%s" cannot be restored because the primary key column "%s" is nullable.',
                            $tableInfo['name'],
                            $columnName,
                        ));
                    }
                }
            }
        }
    }
}
