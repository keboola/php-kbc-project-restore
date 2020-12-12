<?php

declare(strict_types=1);

namespace Keboola\ProjectRestore;

use Keboola\Csv\CsvFile;
use Keboola\ProjectRestore\StorageApi\BucketInfo;
use Keboola\ProjectRestore\StorageApi\ConfigurationFilter;
use Keboola\ProjectRestore\StorageApi\Token;
use Keboola\StorageApi\Client as StorageApi;
use Aws\S3\S3Client;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\ClientException as StorageApiException;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\Temp\Temp;
use Psr\Log\NullLogger;
use Psr\Log\LoggerInterface;
use Symfony\Component\Filesystem\Filesystem;

class S3Restore
{
    /**
     * @var StorageApi
     */
    private $sapiClient;

    /**
     * @var S3Client
     */
    private $s3Client;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var Token
     */
    private $token;

    public function __construct(S3Client $s3Client, StorageApi $sapiClient, ?LoggerInterface $logger = null)
    {
        $this->s3Client = $s3Client;
        $this->sapiClient = $sapiClient;
        $this->token = new Token($this->sapiClient);
        $this->logger = $logger?: new NullLogger();
    }

    private function trimSourceBasePath(?string $targetBasePath = null): string
    {
        if (empty($targetBasePath) || $targetBasePath === '/') {
            return '';
        } else {
            return trim($targetBasePath, '/') . '/';
        }
    }

    public function restoreTableAliases(string $sourceBucket, ?string $sourceBasePath = null): void
    {
        $sourceBasePath = $this->trimSourceBasePath($sourceBasePath);
        $this->logger->info('Downloading tables');

        $tmp = new Temp();
        $tmp->initRunFolder();

        $targetFile = $tmp->createFile('tables.json');
        $this->s3Client->getObject([
            'Bucket' => $sourceBucket,
            'Key' => $sourceBasePath . 'tables.json',
            'SaveAs' => $targetFile->getPathname(),
        ]);

        $tables = json_decode((string) file_get_contents($targetFile->getPathname()), true);
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

            // Alias attributes
            if (isset($tableInfo['attributes']) && count($tableInfo['attributes'])) {
                $this->sapiClient->replaceTableAttributes($tableId, $tableInfo['attributes']);
            }
            if (isset($tableInfo['metadata']) && count($tableInfo['metadata'])) {
                foreach ($this->prepareMetadata($tableInfo['metadata']) as $provider => $metadata) {
                    $metadataClient->postTableMetadata($tableId, $provider, $metadata);
                }
            }
            if (isset($tableInfo['columnMetadata']) && count($tableInfo['columnMetadata'])) {
                foreach ($tableInfo['columnMetadata'] as $column => $columnMetadata) {
                    foreach ($this->prepareMetadata($columnMetadata) as $provider => $metadata) {
                        $metadataClient->postColumnMetadata($tableId . '.' . $column, $provider, $metadata);
                    }
                }
            }
        }
    }

    public function restoreTables(string $sourceBucket, ?string $sourceBasePath = null): void
    {
        $sourceBasePath = $this->trimSourceBasePath($sourceBasePath);
        $this->logger->info('Downloading tables');

        $tmp = new Temp();
        $tmp->initRunFolder();

        $targetFile = $tmp->createFile('tables.json');
        $this->s3Client->getObject([
            'Bucket' => $sourceBucket,
            'Key' => $sourceBasePath . 'tables.json',
            'SaveAs' => $targetFile->getPathname(),
        ]);

        $tables = json_decode((string) file_get_contents($targetFile->getPathname()), true);
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

            //@FIXME
            // create empty table
            $headerFile = $tmp->createFile(sprintf('%s.header.csv', $tableId));
            $headerFile = new CsvFile($headerFile->getPathname());
            $headerFile->writeRow($tableInfo['columns']);

            $tableId = $this->sapiClient->createTableAsync(
                $bucketId,
                $tableInfo['name'],
                $headerFile,
                [
                    'primaryKey' => join(',', $tableInfo['primaryKey']),
                ]
            );

            // Table attributes
            if (isset($tableInfo['attributes']) && count($tableInfo['attributes'])) {
                $this->sapiClient->replaceTableAttributes($tableId, $tableInfo['attributes']);
            }
            if (isset($tableInfo['metadata']) && count($tableInfo['metadata'])) {
                foreach ($this->prepareMetadata($tableInfo['metadata']) as $provider => $metadata) {
                    $metadataClient->postTableMetadata($tableId, $provider, $metadata);
                }
            }
            if (isset($tableInfo['columnMetadata']) && count($tableInfo['columnMetadata'])) {
                foreach ($tableInfo['columnMetadata'] as $column => $columnMetadata) {
                    foreach ($this->prepareMetadata($columnMetadata) as $provider => $metadata) {
                        $metadataClient->postColumnMetadata($tableId . '.' . $column, $provider, $metadata);
                    }
                }
            }

            // upload data
            $iterator = $this->s3Client->getIterator('ListObjects', [
                'Bucket' => $sourceBucket,
                'Prefix' => $sourceBasePath . str_replace('.', '/', $tableId) . '.',
            ]);

            $slices = iterator_to_array($iterator);

            // no files for the table found, probably an empty table
            if (count($slices) === 0) {
                unset($headerFile);
                continue;
            }

            $firstSlice = reset($slices);
            if (count($slices) === 1 && substr($firstSlice['Key'], -14) !== '.part_0.csv.gz') {
                // one file and no slices => the file has header
                // no slices = file does not end with .part_0.csv.gz
                $targetFile = $tmp->createFile(sprintf('%s.csv.gz', $tableId));
                $this->s3Client->getObject(
                    [
                        'Bucket' => $sourceBucket,
                        'Key' => $firstSlice['Key'],
                        'SaveAs' => $targetFile->getPathname(),
                    ]
                );
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

                    $this->s3Client->getObject(
                        [
                            'Bucket' => $sourceBucket,
                            'Key' => $slice['Key'],
                            'SaveAs' => $fileName,
                        ]
                    );
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
     * @param string $sourceBucket
     * @param null|string $sourceBasePath
     * @return BucketInfo[]
     * @throws \Exception
     */
    public function getBucketsInBackup(string $sourceBucket, ?string $sourceBasePath = null): array
    {
        $sourceBasePath = $this->trimSourceBasePath($sourceBasePath);
        $this->logger->info('Downloading buckets');

        $tmp = new Temp();
        $tmp->initRunFolder();

        $targetFile = $tmp->createFile('buckets.json');
        $this->s3Client->getObject([
            'Bucket' => $sourceBucket,
            'Key' => $sourceBasePath . 'buckets.json',
            'SaveAs' => $targetFile->getPathname(),
        ]);

        $buckets = json_decode((string) file_get_contents($targetFile->getPathname()), true);

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
            $useDefaultBackend ? null : $bucket->getBackend()
        );

        // bucket attributes
        if (count($bucket->getAttributes())) {
            $this->sapiClient->replaceBucketAttributes($bucket->getId(), $bucket->getAttributes());
        }

        // bucket metadata
        if (count($bucket->getMetadata())) {
            $metadataClient = new Metadata($this->sapiClient);
            foreach ($this->prepareMetadata($bucket->getMetadata()) as $provider => $metadata) {
                $metadataClient->postBucketMetadata($bucket->getId(), $provider, $metadata);
            }
        }

        return true;
    }

    public function restoreBuckets(
        string $sourceBucket,
        ?string $sourceBasePath = null,
        bool $checkBackend = true
    ): void {
        $buckets = $this->getBucketsInBackup($sourceBucket, $sourceBasePath);

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

            // bucket attributes
            if (count($bucketInfo->getAttributes())) {
                $this->sapiClient->replaceBucketAttributes($bucketInfo->getId(), $bucketInfo->getAttributes());
            }
            if (count($bucketInfo->getMetadata())) {
                foreach ($this->prepareMetadata($bucketInfo->getMetadata()) as $provider => $metadata) {
                    $metadataClient->postBucketMetadata($bucketInfo->getId(), $provider, $metadata);
                }
            }
        }
    }

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

    public function restoreConfigs(
        string $sourceBucket,
        ?string $sourceBasePath = null,
        array $skipComponents = []
    ): void {
        $sourceBasePath = $this->trimSourceBasePath($sourceBasePath);
        $this->logger->info('Downloading configurations');

        $tmp = new Temp();
        $tmp->initRunFolder();

        $targetFile = $tmp->createFile('configurations.json');
        $this->s3Client->getObject([
            'Bucket' => $sourceBucket,
            'Key' => $sourceBasePath . 'configurations.json',
            'SaveAs' => $targetFile->getPathname(),
        ]);

        $configurations = json_decode((string) file_get_contents($targetFile->getPathname()), true);

        $components = new Components($this->sapiClient);

        $componentList = [];
        foreach ($this->sapiClient->indexAction()['components'] as $component) {
            $componentList[$component['id']] = $component;
        }

        foreach ($configurations as $componentWithConfigurations) {
            if (in_array($componentWithConfigurations['id'], $skipComponents)) {
                $this->logger->warning(
                    sprintf(
                        'Skipping %s configurations - component marked as skipped',
                        $componentWithConfigurations['id']
                    )
                );
                continue;
            }

            // skip non-existing components
            if (!array_key_exists($componentWithConfigurations['id'], $componentList)) {
                $this->logger->warning(
                    sprintf(
                        'Skipping %s configurations - component does not exists',
                        $componentWithConfigurations['id']
                    )
                );
                continue;
            }

            $this->logger->info(sprintf('Restoring %s configurations', $componentWithConfigurations['id']));

            foreach ($componentWithConfigurations['configurations'] as $componentConfiguration) {
                $targetFile = $tmp->createFile(
                    sprintf(
                        'configurations-%s-%s.json',
                        $componentWithConfigurations['id'],
                        $componentConfiguration['id']
                    )
                );
                $this->s3Client->getObject(
                    [
                        'Bucket' => $sourceBucket,
                        'Key' => sprintf(
                            '%sconfigurations/%s/%s.json',
                            $sourceBasePath,
                            $componentWithConfigurations['id'],
                            $componentConfiguration['id']
                        ),
                        'SaveAs' => $targetFile->getPathname(),
                    ]
                );

                // configurations as objects to preserve empty arrays or empty objects
                $configurationData = json_decode((string) file_get_contents($targetFile->getPathname()));

                // create empty configuration
                $configuration = new Configuration();
                $configuration->setComponentId($componentWithConfigurations['id']);
                $configuration->setConfigurationId($componentConfiguration['id']);
                $configuration->setDescription($configurationData->description);
                $configuration->setName($configurationData->name);
                $components->addConfiguration($configuration);

                // update configuration and state
                $configuration->setChangeDescription(sprintf(
                    'Configuration %s restored from backup',
                    $componentConfiguration['id']
                ));
                $configuration->setConfiguration(
                    ConfigurationFilter::removeOauthAuthorization($configurationData->configuration)
                );
                if (isset($configurationData->state)) {
                    $configuration->setState($configurationData->state);
                }
                $components->updateConfiguration($configuration);

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
                        if (isset($row->state)) {
                            $configurationRow->setState($row->state);
                        }
                        $components->updateConfigurationRow($configurationRow);
                    }
                }

                // restore row sorting
                if (!empty($configurationData->rowsSortOrder)) {
                    $configuration->setRowsSortOrder($configurationData->rowsSortOrder);
                    $configuration->setChangeDescription('Restored rows sort order from backup');
                    $components->updateConfiguration($configuration);
                }
            }
        }
    }

    public function listConfigsInBackup(
        string $sourceBucket,
        ?string $sourceBasePath = null,
        string $componentId
    ): array {
        $sourceBasePath = $this->trimSourceBasePath($sourceBasePath);
        $this->logger->info('Downloading configurations');

        $tmp = new Temp();
        $tmp->initRunFolder();

        $targetFile = $tmp->createFile('configurations.json');
        $this->s3Client->getObject([
            'Bucket' => $sourceBucket,
            'Key' => $sourceBasePath . 'configurations.json',
            'SaveAs' => (string) $targetFile,
        ]);

        $components = json_decode((string) file_get_contents((string) $targetFile), true);
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
}
