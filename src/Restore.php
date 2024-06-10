<?php

declare(strict_types=1);

namespace Keboola\ProjectRestore;

use Keboola\Csv\CsvFile;
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
use Keboola\Temp\Temp;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

abstract class Restore
{
    protected Client $sapiClient;

    protected BranchAwareClient $branchAwareClient;

    protected LoggerInterface $logger;

    protected Token $token;

    public function __construct(?LoggerInterface $logger = null, Client $sapiClient)
    {
        $this->sapiClient = $sapiClient;
        $this->token = new Token($this->sapiClient);
        $this->logger = $logger?: new NullLogger();

        $devBranches = new DevBranches($this->sapiClient);
        $listBranches = $devBranches->listBranches();
        $defaultBranch = current(array_filter($listBranches, fn($v) => $v['isDefault'] === true));

        $this->branchAwareClient = new BranchAwareClient(
            $defaultBranch['id'],
            [
                'url' => $sapiClient->getApiUrl(),
                'token' => $sapiClient->getTokenString(),
            ]
        );
    }

    public function restoreProjectMetadata(): void
    {
        $devBranchMetadata = new DevBranchesMetadata($this->branchAwareClient);

        $fileContent = $this->getDataFromStorage('defaultBranchMetadata.json');
        $projectMetadata = json_decode((string) $fileContent, true);

        if ($projectMetadata) {
            $devBranchMetadata->addBranchMetadata($projectMetadata);
        }
    }

    public function restoreConfigs(array $skipComponents = []): void
    {
        $this->logger->info('Downloading configurations');

        $tmp = new Temp();
        $tmp->initRunFolder();

        $fileContent = $this->getDataFromStorage('configurations.json');
        $configurations = json_decode((string) $fileContent, true);

        $components = new Components($this->sapiClient);

        $componentList = [];
        foreach ($this->sapiClient->indexAction()['components'] as $component) {
            $componentList[$component['id']] = $component;
        }
        foreach ($configurations as $componentWithConfigurations) {
            $componentId = $componentWithConfigurations['id'];

            if (in_array($componentId, $skipComponents, true)) {
                $this->logger->warning(
                    sprintf(
                        'Skipping %s configurations - component marked as skipped',
                        $componentId
                    )
                );
                continue;
            }

            // skip non-existing components
            if (!array_key_exists($componentId, $componentList)) {
                $this->logger->warning(
                    sprintf(
                        'Skipping %s configurations - component does not exists',
                        $componentId
                    )
                );
                continue;
            }

            $this->logger->info(sprintf('Restoring %s configurations', $componentId));

            // restore configuration metadata
            $componentConfigurationsFiles = $this->listComponentConfigurationsFiles(sprintf(
                'configurations/%s',
                $componentId
            ));

            foreach ($componentWithConfigurations['configurations'] as $componentConfiguration) {
                // configurations as objects to preserve empty arrays or empty objects
                $configurationData = json_decode((string) $this->getDataFromStorage(sprintf(
                    'configurations/%s/%s.json',
                    $componentId,
                    $componentConfiguration['id']
                )));

                // create empty configuration
                $configuration = new Configuration();
                $configuration->setComponentId($componentId);
                $configuration->setConfigurationId($componentConfiguration['id']);
                $configuration->setDescription($configurationData->description);
                $configuration->setName($configurationData->name);
                $components->addConfiguration($configuration);

                // update configuration and state
                $configuration->setChangeDescription(sprintf(
                    'Configuration %s restored from backup',
                    $componentConfiguration['id']
                ));

                $configCorrector = new ConfigurationCorrector($this->sapiClient->getApiUrl(), $this->logger);
                $configuration->setConfiguration(
                    $configCorrector->correct($componentId, $configurationData->configuration)
                );

                $components->updateConfiguration($configuration);

                if (isset($configurationData->state)) {
                    $configurationState = new ConfigurationState();
                    $configurationState->setComponentId($componentId);
                    $configurationState->setConfigurationId($componentConfiguration['id']);
                    $configurationState->setState($configurationData->state);
                    $components->updateConfigurationState($configurationState);
                }

                // create configuration rows
                if (count($configurationData->rows)) {
                    foreach ($configurationData->rows as $row) {
                        // create empty row
                        $configurationRow = new ConfigurationRow($configuration);
                        $configurationRow->setRowId($row->id);
                        $components->addConfigurationRow($configurationRow);

                        // update row configuration and state
                        $configurationRow->setConfiguration($row->configuration);
                        $configurationRow->setChangeDescription(sprintf('Row %s restored from backup', $row->id));
                        $configurationRow->setName($row->name);
                        $configurationRow->setDescription($row->description);
                        $configurationRow->setIsDisabled($row->isDisabled);

                        $components->updateConfigurationRow($configurationRow);

                        if (isset($row->state)) {
                            $configurationRowState = new ConfigurationRowState($configuration);
                            $configurationRowState->setRowId($configurationRow->getRowId());
                            $configurationRowState->setState($row->state);
                            $components->updateConfigurationRowState($configurationRowState);
                        }
                    }
                }

                // restore row sorting
                if (!empty($configurationData->rowsSortOrder)) {
                    $configuration->setRowsSortOrder($configurationData->rowsSortOrder);
                    $configuration->setChangeDescription('Restored rows sort order from backup');
                    $components->updateConfiguration($configuration);
                }

                // restore configuration metadata
                $metadataFilePath = sprintf(
                    'configurations/%s/%s.json.metadata',
                    $componentId,
                    $componentConfiguration['id']
                );

                if (in_array($metadataFilePath, $componentConfigurationsFiles, true)) {
                    $metadataData = json_decode((string) $this->getDataFromStorage($metadataFilePath), true);
                    array_walk($metadataData, function (&$v): void {
                        unset($v['id']);
                        unset($v['timestamp']);
                    });

                    $branchAwareComponents = new Components($this->branchAwareClient);

                    $configMetadata = new ConfigurationMetadata($configuration);
                    $configMetadata->setMetadata($metadataData);

                    $branchAwareComponents->addConfigurationMetadata($configMetadata);
                }
            }
        }
    }

    public function getBucketsInBackup(): array
    {
        $this->logger->info('Downloading buckets');

        $tmp = new Temp();
        $tmp->initRunFolder();

        $fileContent = $this->getDataFromStorage('buckets.json');

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

        $this->sapiClient->createBucket(
            substr($bucket->getName(), 2),
            $bucket->getStage(),
            $bucket->getDescription() ?: '',
            $useDefaultBackend ? null : $bucket->getBackend(),
            $bucket->getDisplayName()
        );

        // bucket metadata
        if (count($bucket->getMetadata())) {
            $metadataClient = new Metadata($this->sapiClient);
            foreach ($this->prepareMetadata($bucket->getMetadata()) as $provider => $metadata) {
                $metadataClient->postBucketMetadata($bucket->getId(), (string) $provider, $metadata);
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
                    $metadataClient->postBucketMetadata($bucketInfo->getId(), (string) $provider, $metadata);
                }
            }
        }
    }

    public function restoreTableAliases(): void
    {
        $this->logger->info('Downloading tables');

        $tmp = new Temp();
        $tmp->initRunFolder();

        $fileContent = $this->getDataFromStorage('tables.json');
        $tables = json_decode((string) $fileContent, true);

        $restoredBuckets = array_map(
            function ($bucket) {
                return $bucket['id'];
            },
            $this->sapiClient->listBuckets()
        );
        $metadataClient = new Metadata($this->sapiClient);

        foreach ($tables as $tableInfo) {
            if ($tableInfo['isAlias'] !== true) {
                continue;
            }

            $tableId = $tableInfo['id'];
            $bucketId = $tableInfo['bucket']['id'];

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
            $this->sapiClient->createAliasTable(
                $bucketId,
                $tableInfo['sourceTable']['id'],
                $tableInfo['name'],
                $aliasOptions
            );

            $this->restoreTableColumnsMetadata($tableInfo, $tableId, $metadataClient);
        }
    }

    public function restoreTables(): void
    {
        $this->logger->info('Downloading tables');

        $tmp = new Temp();
        $tmp->initRunFolder();

        $fileContent = $this->getDataFromStorage('tables.json');
        $tables = json_decode((string) $fileContent, true);

        $restoredBuckets = array_map(
            function ($bucket) {
                return $bucket['id'];
            },
            $this->sapiClient->listBuckets()
        );
        $metadataClient = new Metadata($this->sapiClient);

        foreach ($tables as $tableInfo) {
            if ($tableInfo['isAlias'] === true) {
                continue;
            }

            $tableId = $tableInfo['id'];
            $bucketId = $tableInfo['bucket']['id'];

            if (!in_array($bucketId, $restoredBuckets)) {
                $this->logger->warning(sprintf('Skipping table %s', $tableId));
                continue;
            }

            $this->logger->info(sprintf('Restoring table %s', $tableId));

            $headerFile = $tmp->createFile(sprintf('%s.header.csv', $tableInfo['id']));
            $headerFile = new CsvFile($headerFile->getPathname());
            $headerFile->writeRow($tableInfo['columns']);

            $isTyped = $tableInfo['isTyped'] ?? false;
            if ($isTyped) {
                $tableId = $this->restoreTypedTable($tableInfo);
            } else {
                $tableId = $this->restoreTable($tableInfo, $headerFile);
            }

            $this->restoreTableColumnsMetadata($tableInfo, $tableId, $metadataClient);

            // upload data
            $slices = $this->listTableFiles($tableId);

            // no files for the table found, probably an empty table
            if (count($slices) === 0) {
                unset($headerFile);
                continue;
            }

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
                    ]
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
                    ->setIsEncrypted(true)
                ;

                $dataFileId = $this->sapiClient->uploadSlicedFile($downloadedSlices, $fileUploadOptions);

                // Upload data to table
                $this->sapiClient->writeTableAsyncDirect(
                    $tableId,
                    array(
                        'dataFileId' => $dataFileId,
                        'columns' => $headerFile->getHeader()
                    )
                );
            }
            unset($headerFile);
        }
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

        $tmp = new Temp();
        $tmp->initRunFolder();

        $fileContent = $this->getDataFromStorage('configurations.json');

        $components = json_decode((string) $fileContent, true);
        foreach ($components as $component) {
            if ($component['id'] !== $componentId) {
                continue;
            }

            return array_map(
                function (array $configuration) {
                    return $configuration['id'];
                },
                $component['configurations']
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

    private function restoreTable(array $tableInfo, CsvFile $headerFile): string
    {
        // create empty table
        return $this->sapiClient->createTableAsync(
            $tableInfo['bucket']['id'],
            $tableInfo['name'],
            $headerFile,
            [
                'primaryKey' => join(',', $tableInfo['primaryKey']),
            ]
        );
    }

    private function restoreTypedTable(array $tableInfo): string
    {
        $columns = [];
        foreach ($tableInfo['columns'] as $column) {
            $columns[$column] = [];
        }
        foreach ($tableInfo['columnMetadata'] ?? [] as $columnName => $column) {
            $columnMetadata = [];
            foreach ($column as $metadata) {
                if ($metadata['provider'] !== 'storage') {
                    continue;
                }
                $columnMetadata[$metadata['key']] = $metadata['value'];
            }

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

        try {
            return $this->sapiClient->createTableDefinition(
                $tableInfo['bucket']['id'],
                $data
            );
        } catch (ClientException $e) {
            if ($e->getCode() === 400
                && preg_match('/Primary keys columns must be set nullable false/', $e->getMessage())) {
                throw new StorageApiException(sprintf(
                    'Table "%s" cannot be restored because the primary key cannot be set on a nullable column.',
                    $tableInfo['name'],
                ));
            }
            throw $e;
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

        foreach ($metadatas as $provider => $metadata) {
            if ($provider === 'storage') {
                continue;
            }
            $tableMetadataUpdateOptions = new TableMetadataUpdateOptions(
                $tableId,
                (string) $provider,
                $metadata['table'] ?? null,
                $metadata['columns'] ?? null
            );

            $metadataClient->postTableMetadataWithColumns($tableMetadataUpdateOptions);
        }
    }
}
