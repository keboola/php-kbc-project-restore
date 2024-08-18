<?php

declare(strict_types=1);

namespace Keboola\ProjectRestore\Tests;

use Aws\S3\S3Client;
use Keboola\ProjectRestore\AbsRestore;
use Keboola\ProjectRestore\StorageApi\BucketInfo;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\DevBranchesMetadata;
use Keboola\StorageApi\Exception;
use Keboola\ProjectRestore\S3Restore;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\Components\ListConfigurationMetadataOptions;
use Keboola\StorageApi\TableExporter;
use Keboola\Temp\Temp;
use PHPUnit\Framework\Assert;
use stdClass;

class S3RestoreTest extends BaseTest
{
    public const TEST_ITERATOR_SLICES_COUNT = 1222;

    protected S3Client $s3Client;

    public function setUp(): void
    {
        parent::setUp();

        putenv('AWS_ACCESS_KEY_ID=' . getenv('TEST_AWS_ACCESS_KEY_ID'));
        putenv('AWS_SECRET_ACCESS_KEY=' . getenv('TEST_AWS_SECRET_ACCESS_KEY'));

        $this->s3Client = new S3Client([
            'version' => 'latest',
            'region' => getenv('TEST_AWS_REGION'),
            'suppress_php_deprecation_warning' => true,
        ]);
    }

    public function testProjectMetadataRestore(): void
    {
        $metadata = new DevBranchesMetadata($this->branchAwareClient);
        $metadataList = $metadata->listBranchMetadata();
        foreach ($metadataList as $item) {
            $metadata->deleteBranchMetadata((int) $item['id']);
        }

        $restore = new S3Restore(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'branches-metadata'
        );

        $restore->restoreProjectMetadata();

        $metadataList = $metadata->listBranchMetadata();

        self::assertEquals(1, count($metadataList));
        self::assertEquals('KBC.projectDescription', $metadataList[0]['key']);
        self::assertEquals('project description', $metadataList[0]['value']);
    }

    public function testProjectEmptyMetadataRestore(): void
    {
        $metadata = new DevBranchesMetadata($this->branchAwareClient);
        $metadataList = $metadata->listBranchMetadata();
        foreach ($metadataList as $item) {
            $metadata->deleteBranchMetadata((int) $item['id']);
        }

        $restore = new S3Restore(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'branches-empty-metadata'
        );

        $restore->restoreProjectMetadata();

        $metadataList = $metadata->listBranchMetadata();
        self::assertEquals(0, count($metadataList));
    }

    public function testBucketsInBackup(): void
    {
        $backup = new S3Restore(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'buckets'
        );

        $buckets = $backup->getBucketsInBackup();

        self::assertCount(2, $buckets);

        foreach ($buckets as $bucketInfo) {
            $this->assertInstanceOf(BucketInfo::class, $bucketInfo);
        }

        self::assertEquals('in.c-bucket1', $buckets[0]->getId());
        self::assertEquals('in.c-bucket2', $buckets[1]->getId());
    }

    public function testBucketRestore(): void
    {
        $backup = new S3Restore(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'buckets'
        );

        $buckets = $backup->getBucketsInBackup();
        foreach ($buckets as $bucketInfo) {
            $backup->restoreBucket($bucketInfo);
        }

        $buckets = $this->sapiClient->listBuckets();
        self::assertCount(2, $buckets);
        self::assertEquals('in.c-bucket1', $buckets[0]['id']);
        self::assertEquals('in.c-bucket2', $buckets[1]['id']);
    }

    public function testBucketMetadataRestore(): void
    {
        $backup = new S3Restore(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'metadata'
        );

        $buckets = $backup->getBucketsInBackup();
        foreach ($buckets as $bucketInfo) {
            $backup->restoreBucket($bucketInfo);
        }

        $buckets = $this->sapiClient->listBuckets();
        self::assertCount(1, $buckets);
        self::assertEquals('in.c-bucket', $buckets[0]['id']);

        // metadata check
        $this->sapiClient->getBucket('in.c-bucket');

        $metadata = new Metadata($this->sapiClient);
        $bucketMetadata = $metadata->listBucketMetadata('in.c-bucket');

        self::assertCount(1, $bucketMetadata);

        self::assertArrayHasKey('key', $bucketMetadata[0]);
        self::assertEquals('bucketKey', $bucketMetadata[0]['key']);

        self::assertArrayHasKey('value', $bucketMetadata[0]);
        self::assertEquals('bucketValue', $bucketMetadata[0]['value']);

        self::assertArrayHasKey('provider', $bucketMetadata[0]);
        self::assertEquals('system', $bucketMetadata[0]['provider']);
    }

    public function testBucketDefaultBackendRestore(): void
    {
        $backup = new S3Restore(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'buckets-multiple-backends'
        );

        $buckets = $backup->getBucketsInBackup();
        foreach ($buckets as $bucketInfo) {
            $backup->restoreBucket($bucketInfo, true);
        }

        $buckets = $this->sapiClient->listBuckets();
        self::assertCount(2, $buckets);
        self::assertTrue($this->sapiClient->bucketExists('in.c-snowflake'));
        self::assertTrue($this->sapiClient->bucketExists('in.c-redshift'));
    }

    public function testBucketMissingBackend(): void
    {
        $backup = new S3Restore(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'buckets-multiple-backends'
        );

        $tokenData = $this->sapiClient->verifyToken();
        $projectData = $tokenData['owner'];

        $buckets = $backup->getBucketsInBackup();
        self::assertCount(2, $buckets);

        $fails = 0;
        foreach ($buckets as $bucketInfo) {
            if ($bucketInfo->getBackend() === $projectData['defaultBackend']) {
                continue;
            }

            try {
                $backup->restoreBucket($bucketInfo);
                self::fail('Restoring bucket with non-supported backend should fail');
            } catch (ClientException $e) {
                $message1 = 'is not supported for project';
                $message2 = 'was not found in the haystack';

                self::assertTrue(
                    strpos($e->getMessage(), $message1) !== false
                    || strpos($e->getMessage(), $message2) !== false
                );
                $fails++;
            }
        }

        self::assertGreaterThan(0, $fails);
    }


    public function testBucketWithoutPrefixRestore(): void
    {
        $backup = new S3Restore(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'bucket-without-prefix'
        );
        $buckets = $backup->getBucketsInBackup();

        try {
            $backup->restoreBucket(reset($buckets));
            self::fail('Restoring bucket with non-supported backend should fail');
        } catch (Exception $e) {
            self::assertStringContainsString('System bucket restore is not supported', $e->getMessage());
        }
    }

    public function testBucketLinkRestore(): void
    {
        $backup = new S3Restore(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'buckets-linked-bucket'
        );
        $buckets = $backup->getBucketsInBackup();

        $fails = 0;
        foreach ($buckets as $bucketInfo) {
            if (!$bucketInfo->isLinkedBucket()) {
                continue;
            }

            try {
                $backup->restoreBucket($bucketInfo);
                self::fail('Restoring bucket with non-supported backend should fail');
            } catch (Exception $e) {
                self::assertStringContainsString('Linked bucket restore is not supported', $e->getMessage());
                $fails++;
            }
        }

        self::assertGreaterThan(0, $fails);
    }

    public function testPermanentFilesRestore(): void
    {
        $files = $this->sapiClient->listFiles();
        foreach ($files as $file) {
            $this->sapiClient->deleteFile($file['id']);
        }
        $restore = new S3Restore(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'permanent-files'
        );
        $restore->restorePermanentFiles();

        $files = $this->sapiClient->listFiles();
        $permanentFiles = array_filter($files, function ($file) {
            return is_null($file['maxAgeDays']);
        });

        self::assertCount(1, $permanentFiles);
        self::assertEquals(['tag1', 'tag2'], $permanentFiles[0]['tags']);
    }

    public function testListConfigsInBackup(): void
    {
        $backup = new S3Restore(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'configurations'
        );

        $componentId = 'keboola.csv-import';
        $configs = $backup->listConfigsInBackup($componentId);

        self::assertCount(1, $configs);
        self::assertEquals('213957449', reset($configs));

        // component not in backup
        $componentId = 'orchestrator';
        $configs = $backup->listConfigsInBackup($componentId);

        self::assertTrue(is_array($configs));
        self::assertCount(0, $configs);
    }

    public function testRestoreBuckets(): void
    {
        $backup = new S3Restore(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'buckets'
        );
        $backup->restoreBuckets(true);

        $buckets = $this->sapiClient->listBuckets();
        self::assertCount(2, $buckets);
        self::assertEquals('in.c-bucket1', $buckets[0]['id']);
        self::assertEquals('in.c-bucket2', $buckets[1]['id']);
    }

    public function testRestoreLinkedBuckets(): void
    {
        $backup = new S3Restore(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'buckets-linked-bucket'
        );
        $backup->restoreBuckets(true);
        $backup->restoreTables();
        $backup->restoreTableAliases();

        $buckets = $this->sapiClient->listBuckets();
        self::assertCount(2, $buckets);
        self::assertEquals('in.c-bucket1', $buckets[0]['id']);
        self::assertEquals('in.c-bucket2', $buckets[1]['id']);

        $tables = $this->sapiClient->listTables($buckets[0]['id']);
        self::assertCount(1, $tables);
        self::assertEquals('in.c-bucket1.sample', $tables[0]['id']);
    }

    public function testRestoreBucketsIgnoreStorageBackend(): void
    {
        $backup = new S3Restore(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'buckets-multiple-backends'
        );
        $backup->restoreBuckets(false);

        $buckets = $this->sapiClient->listBuckets();
        self::assertCount(2, $buckets);
        self::assertTrue($this->sapiClient->bucketExists('in.c-snowflake'));
        self::assertTrue($this->sapiClient->bucketExists('in.c-redshift'));
    }

    public function testBackendMissingError(): void
    {
        $backup = new S3Restore(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'buckets-multiple-backends'
        );

        try {
            $backup->restoreBuckets(true);
            self::fail('Restoring buckets with non-supported backends should fail');
        } catch (Exception $e) {
            self::assertStringContainsString('Missing', $e->getMessage());
            self::assertStringContainsString('backend', $e->getMessage());
        }
    }

    public function testRestoreTableWithHeader(): void
    {
        $backup = new S3Restore(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'table-with-header'
        );
        $backup->restoreBuckets(true);
        $backup->restoreTables();

        $temp = new Temp();
        $temp->initRunFolder();

        self::assertTrue($this->sapiClient->tableExists('in.c-bucket.Account'));

        $tableExporter = new TableExporter($this->sapiClient);
        $file = $temp->createFile('account.csv');
        $tableExporter->exportTable('in.c-bucket.Account', $file->getPathname(), []);
        $fileContents = file_get_contents($file->getPathname());
        self::assertContains('"Id","Name"', $fileContents);
        self::assertContains('"001C000000xYbhhIAC","Keboola"', $fileContents);
        self::assertContains('"001C000000xYbhhIAD","Keboola 2"', $fileContents);
    }

    public function testRestoreTableWithoutHeader(): void
    {
        $backup = new S3Restore(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'table-without-header'
        );
        $backup->restoreBuckets(true);
        $backup->restoreTables();

        $temp = new Temp();
        $temp->initRunFolder();

        self::assertTrue($this->sapiClient->tableExists('in.c-bucket.Account'));

        $tableExporter = new TableExporter($this->sapiClient);
        $file = $temp->createFile('account.csv');
        $tableExporter->exportTable('in.c-bucket.Account', $file->getPathname(), []);
        $fileContents = file_get_contents($file->getPathname());
        self::assertContains('"Id","Name"', $fileContents);
        self::assertContains('"001C000000xYbhhIAC","Keboola"', $fileContents);
        self::assertContains('"001C000000xYbhhIAD","Keboola 2"', $fileContents);
    }

    public function testRestoreTableFromMultipleSlices(): void
    {
        $backup = new S3Restore(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'table-multiple-slices'
        );
        $backup->restoreBuckets(true);
        $backup->restoreTables();

        $temp = new Temp();
        $temp->initRunFolder();

        self::assertTrue($this->sapiClient->tableExists('in.c-bucket.Account'));
        $tableExporter = new TableExporter($this->sapiClient);
        $file = $temp->createFile('account.csv');
        $tableExporter->exportTable('in.c-bucket.Account', $file->getPathname(), []);
        $fileContents = file_get_contents($file->getPathname());
        self::assertStringContainsString('"Id","Name"', (string) $fileContents);
        self::assertStringContainsString('"001C000000xYbhhIAC","Keboola"', (string) $fileContents);
        self::assertStringContainsString('"001C000000xYbhhIAD","Keboola 2"', (string) $fileContents);
    }

    public function testRestoreTableFromLargeAmountOfSlices(): void
    {
        $sourceBucket = sprintf('table-%s-slices', self::TEST_ITERATOR_SLICES_COUNT);
        $backup = new S3Restore(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            $sourceBucket
        );

        $backup->restoreBuckets(true);
        $backup->restoreTables();

        $table = $this->sapiClient->getTable('in.c-bucket.Account');
        $this->assertEquals(self::TEST_ITERATOR_SLICES_COUNT, $table['rowsCount']);
    }

    public function testRestoreTableFromMultipleSlicesSharedPrefix(): void
    {
        $backup = new S3Restore(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'table-multiple-slices-shared-prefix'
        );
        $backup->restoreBuckets(true);
        $backup->restoreTables();

        $temp = new Temp();
        $temp->initRunFolder();

        self::assertTrue($this->sapiClient->tableExists('in.c-bucket.Account'));
        self::assertTrue($this->sapiClient->tableExists('in.c-bucket.Account2'));

        $tableExporter = new TableExporter($this->sapiClient);
        $file = $temp->createFile('account.csv');
        $tableExporter->exportTable('in.c-bucket.Account', $file->getPathname(), []);
        $fileContents = (string) file_get_contents($file->getPathname());
        self::assertStringContainsString('"Id","Name"', $fileContents);
        self::assertStringContainsString('"001C000000xYbhhIAC","Keboola"', $fileContents);
        self::assertStringContainsString('"001C000000xYbhhIAD","Keboola 2"', $fileContents);
        self::assertCount(4, explode("\n", $fileContents));

        $file = $temp->createFile('account2.csv');
        $tableExporter->exportTable('in.c-bucket.Account2', $file->getPathname(), []);
        $fileContents = (string) file_get_contents($file->getPathname());
        self::assertStringContainsString('"Id","Name"', $fileContents);
        self::assertStringContainsString('"001C000000xYbhhIAC","Keboola"', $fileContents);
        self::assertStringContainsString('"001C000000xYbhhIAD","Keboola 2"', $fileContents);
        self::assertCount(4, explode("\n", $fileContents));
    }

    public function testRestoreTablePrimaryKeys(): void
    {
        $backup = new S3Restore(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'table-properties'
        );
        $backup->restoreBuckets(true);
        $backup->restoreTables();

        $accountTable = $this->sapiClient->getTable('in.c-bucket.Account');
        $account2Table = $this->sapiClient->getTable('in.c-bucket.Account2');
        self::assertEquals(['Id', 'Name'], $accountTable['primaryKey']);
        self::assertEquals(['Id'], $account2Table['primaryKey']);
    }

    public function testRestoreNativeDataTypesTable(): void
    {
        $backup = new S3Restore(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'native-data-types-table'
        );
        $backup->restoreBuckets(true);
        $backup->restoreTables();

        $accountTable = $this->sapiClient->getTable('in.c-bucket.firstTable');
        self::assertEquals(['id'], $accountTable['primaryKey']);
        self::assertTrue($accountTable['isTyped']);

        $columnsMetadatas = $accountTable['columnMetadata'];
        self::assertCount(4, $columnsMetadatas);

        $expectedMetadata = [];
        $expectedMetadata['storage'] = [
            'date' => [
                'KBC.datatype.type' => 'DATE',
                'KBC.datatype.nullable' => '',
                'KBC.datatype.basetype' => 'DATE',
            ],
            'datetime' => [
                'KBC.datatype.type' => 'TIMESTAMP_NTZ',
                'KBC.datatype.nullable' => '1',
                'KBC.datatype.basetype' => 'TIMESTAMP',
                'KBC.datatype.length' => '9',
            ],
            'id' => [
                'KBC.datatype.type' => 'NUMBER',
                'KBC.datatype.nullable' => '',
                'KBC.datatype.basetype' => 'NUMERIC',
                'KBC.datatype.length' => '38,0',
            ],
            'name' => [
                'KBC.datatype.type' => 'VARCHAR',
                'KBC.datatype.nullable' => '1',
                'KBC.datatype.basetype' => 'STRING',
                'KBC.datatype.length' => '255',
            ],
        ];
        $expectedMetadata['redshift'] = [
            'date' => [
                'KBC.datatype.type' => 'VARCHAR',
            ],
        ];
        $expectedMetadata['snowflake'] = [
            'id' => [
                'KBC.datatype.type' => 'INT',
            ],
        ];
        $expectedMetadata['mysql'] = [
            'name' => [
                'KBC.datatype.type' => 'TEXT',
            ],
        ];

        $countSavedMetadata = 0;
        foreach ($columnsMetadatas as $column => $columnMetadatas) {
            foreach ($columnMetadatas as $columnMetadata) {
                $provider = $columnMetadata['provider'];
                $key = $columnMetadata['key'];

                Assert::assertArrayHasKey($provider, $expectedMetadata, $column);
                Assert::assertArrayHasKey($column, $expectedMetadata[$provider], $column);
                /** @phpstan-ignore-next-line */
                Assert::assertArrayHasKey($key, $expectedMetadata[$provider][$column], $column);
                /** @phpstan-ignore-next-line */
                Assert::assertEquals($expectedMetadata[$provider][$column][$key], $columnMetadata['value'], $column);
                $countSavedMetadata++;
            }
        }
        Assert::assertEquals(18, $countSavedMetadata);
    }

    public function testRestoreAlias(): void
    {
        $backup = new S3Restore(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'alias'
        );
        $backup->restoreBuckets(true);
        $backup->restoreTables();
        $backup->restoreTableAliases();

        $aliasTable = $this->sapiClient->getTable('out.c-bucket.Account');
        self::assertEquals(true, $aliasTable['isAlias']);
        self::assertEquals(true, $aliasTable['aliasColumnsAutoSync']);
        self::assertEquals(['Id', 'Name'], $aliasTable['columns']);
        self::assertEquals('in.c-bucket.Account', $aliasTable['sourceTable']['id']);
    }

    public function testRestoreAliasMetadata(): void
    {
        $backup = new S3Restore(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'alias-metadata'
        );
        $backup->restoreBuckets(true);
        $backup->restoreTables();
        $backup->restoreTableAliases();

        self::assertTrue($this->sapiClient->tableExists('out.c-bucket.Account'));

        $aliasTable = $this->sapiClient->getTable('out.c-bucket.Account');
        self::assertEquals('tableKey', $aliasTable['metadata'][0]['key']);
        self::assertEquals('tableValue', $aliasTable['metadata'][0]['value']);
        self::assertEquals('columnKey', $aliasTable['columnMetadata']['Id'][0]['key']);
        self::assertEquals('columnValue', $aliasTable['columnMetadata']['Id'][0]['value']);
    }

    public function testRestoreFilteredAlias(): void
    {
        $backup = new S3Restore(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'alias-filtered'
        );
        $backup->restoreBuckets(true);
        $backup->restoreTables();
        $backup->restoreTableAliases();

        $aliasTable = $this->sapiClient->getTable('out.c-bucket.Account');
        self::assertEquals(true, $aliasTable['isAlias']);
        self::assertEquals(false, $aliasTable['aliasColumnsAutoSync']);
        self::assertEquals(['Id'], $aliasTable['columns']);
        self::assertEquals('in.c-bucket.Account', $aliasTable['sourceTable']['id']);
        self::assertEquals(
            [
                'column' => 'Name',
                'operator' => 'eq',
                'values' => ['Keboola'],
            ],
            $aliasTable['aliasFilter']
        );
    }

    public function testRestoreConfigurations(): void
    {
        $backup = new S3Restore(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'configurations'
        );
        $backup->restoreConfigs();

        $components = new Components($this->sapiClient);
        $componentsList = $components->listComponents();

        self::assertCount(2, $componentsList);
        self::assertEquals('keboola.csv-import', $componentsList[0]['id']);
        self::assertEquals('keboola.ex-slack', $componentsList[1]['id']);

        $config = $components->getConfiguration('keboola.csv-import', '213957449');
        self::assertEquals(1, $config['version']);
        self::assertEquals('Configuration created', $config['changeDescription']);
        self::assertEquals('Accounts', $config['name']);
        self::assertEquals('Default CSV Importer', $config['description']);
        self::assertEquals(['key' => 'value'], $config['state']);
        self::assertEquals(
            json_decode(
                (string) file_get_contents(
                    __DIR__ . '/data/backups/configurations/configurations/keboola.csv-import/213957449.json'
                ),
                true
            )['configuration'],
            $config['configuration']
        );

        $config = $components->getConfiguration('keboola.ex-slack', '213957518');
        $expectedConfigData = json_decode(
            (string) file_get_contents(
                __DIR__ . '/data/backups/configurations/configurations/keboola.ex-slack/213957518.json'
            ),
            true
        )['configuration'];
        $expectedConfigData['authorization']['oauth_api'] = [];
        self::assertEquals(2, $config['version']);
        self::assertEquals('Configuration 213957518 restored from backup', $config['changeDescription']);
        self::assertEmpty($config['state']);
        self::assertEquals($expectedConfigData, $config['configuration']);
    }

    public function testRestoreConfigurationsWithoutVersions(): void
    {
        $backup = new S3Restore(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'configurations-no-versions'
        );
        $backup->restoreConfigs();

        $components = new Components($this->sapiClient);
        $componentsList = $components->listComponents();

        self::assertCount(2, $componentsList);
        self::assertEquals('keboola.csv-import', $componentsList[0]['id']);
        self::assertEquals('keboola.ex-slack', $componentsList[1]['id']);

        $config = $components->getConfiguration('keboola.csv-import', 1);

        self::assertEquals(1, $config['version']);
        self::assertEquals('Configuration created', $config['changeDescription']);
        self::assertEquals('Accounts', $config['name']);
        self::assertEquals('Default CSV Importer', $config['description']);
        self::assertEquals(['key' => 'value'], $config['state']);

        $config = $components->getConfiguration('keboola.ex-slack', 2);
        self::assertEquals(2, $config['version']);
        self::assertEquals('Configuration 2 restored from backup', $config['changeDescription']);
        self::assertEmpty($config['state']);
    }

    public function testSkipComponentsConfigurations(): void
    {
        $backup = new S3Restore(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'configuration-skip'
        );
        $backup->restoreConfigs(
            [
                'gooddata-writer',
                'orchestrator',
                'pigeon-importer',
            ]
        );

        $components = new Components($this->sapiClient);
        $componentsList = $components->listComponents();

        self::assertCount(1, $componentsList);
        self::assertEquals('keboola.csv-import', $componentsList[0]['id']);
        self::assertCount(1, $componentsList[0]['configurations']);
    }

    public function testRestoreEmptyObjectInConfiguration(): void
    {
        $backup = new S3Restore(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'configuration-empty-object'
        );
        $backup->restoreConfigs();

        $temp = new Temp();
        $temp->initRunFolder();

        // empty array and object in config
        $file = $temp->createFile('config.json');
        $this->sapiClient->apiGet('components/keboola.csv-import/configs/1', $file->getPathname());
        $config = json_decode((string) file_get_contents($file->getPathname()));
        self::assertEquals(new stdClass(), $config->configuration->emptyObject);
        self::assertEquals([], $config->configuration->emptyArray);
    }

    public function testRestoreConfigurationRows(): void
    {
        $backup = new S3Restore(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'configuration-rows'
        );
        $backup->restoreConfigs();

        $components = new Components($this->sapiClient);
        $componentsList = $components->listComponents();

        self::assertCount(1, $componentsList);
        self::assertEquals('transformation', $componentsList[0]['id']);
        self::assertCount(2, $componentsList[0]['configurations']);

        $config = $components->getConfiguration('transformation', 1);
        self::assertEquals('MySQL', $config['name']);
        self::assertEquals(6, $config['version']);
        self::assertEquals(['4', '3'], $config['rowsSortOrder']);
        self::assertEquals('Restored rows sort order from backup', $config['changeDescription']);
        self::assertCount(2, $config['rows']);
        self::assertEquals(4, $config['rows'][0]['id']);
        self::assertEquals('Ratings', $config['rows'][0]['configuration']['name']);
        self::assertEquals('Ratings transformation', $config['rows'][0]['name']);
        self::assertEquals('Ratings transformation description', $config['rows'][0]['description']);
        self::assertFalse($config['rows'][0]['isDisabled']);
        self::assertEmpty($config['rows'][0]['state']);
        self::assertEquals(3, $config['rows'][1]['id']);
        self::assertEquals('Account', $config['rows'][1]['configuration']['name']);
        self::assertEquals('Account transformation', $config['rows'][1]['name']);
        self::assertEquals('Account transformation description', $config['rows'][1]['description']);
        self::assertTrue($config['rows'][1]['isDisabled']);
        self::assertEquals(['rowKey' => 'value'], $config['rows'][1]['state']);

        $config = $components->getConfiguration('transformation', 2);
        self::assertEquals('Snowflake', $config['name']);
        self::assertEquals(5, $config['version']);
        self::assertEmpty($config['rowsSortOrder']);
        self::assertEquals('Row 6 restored from backup', $config['changeDescription']);
        self::assertEquals(5, $config['rows'][0]['id']);
        self::assertEquals('Account', $config['rows'][0]['configuration']['name']);
        self::assertEquals('Account transformation', $config['rows'][0]['name']);
        self::assertEquals('Account transformation description', $config['rows'][0]['description']);
        self::assertTrue($config['rows'][0]['isDisabled']);
        self::assertEmpty($config['rows'][0]['state']);
        self::assertEquals(6, $config['rows'][1]['id']);
        self::assertEquals('Ratings', $config['rows'][1]['configuration']['name']);
        self::assertEquals('Ratings transformation', $config['rows'][1]['name']);
        self::assertEquals('Ratings transformation description', $config['rows'][1]['description']);
        self::assertFalse($config['rows'][1]['isDisabled']);
        self::assertEmpty($config['rows'][1]['state']);
    }

    public function testRestoreEmptyObjectInConfigurationRow(): void
    {
        $backup = new S3Restore(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'configuration-rows'
        );
        $backup->restoreConfigs();

        $temp = new Temp();
        $temp->initRunFolder();

        // empty array and object in config rows
        $file = $temp->createFile('config.json');
        $this->sapiClient->apiGet('components/transformation/configs/1/rows', $file->getPathname());
        $config = json_decode((string) file_get_contents($file->getPathname()));
        self::assertEquals(new stdClass(), $config[0]->configuration->input[0]->datatypes);
        self::assertEquals([], $config[0]->configuration->queries);
    }

    public function testRestoreBucketWithoutPrefix(): void
    {
        $backup = new S3Restore(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'bucket-without-prefix'
        );
        $backup->restoreBuckets(true);

        $buckets = $this->sapiClient->listBuckets();
        self::assertCount(0, $buckets);
    }

    public function testRestoreTableWithoutPrefix(): void
    {
        $backup = new S3Restore(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'table-without-prefix'
        );
        $backup->restoreBuckets(true);
        $backup->restoreTables();

        $buckets = $this->sapiClient->listBuckets();
        self::assertCount(0, $buckets);
    }

    public function testRestoreTableEmpty(): void
    {
        $backup = new S3Restore(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'table-empty'
        );
        $backup->restoreBuckets(true);
        $backup->restoreTables();

        self::assertTrue($this->sapiClient->tableExists('in.c-bucket.Account'));
    }

    public function testRestoreMetadata(): void
    {
        $backup = new S3Restore(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'metadata'
        );
        $backup->restoreBuckets(true);
        $backup->restoreTables();

        self::assertTrue($this->sapiClient->tableExists('in.c-bucket.Account'));

        $table = $this->sapiClient->getTable('in.c-bucket.Account');

        self::assertEquals('tableKey', $table['metadata'][0]['key']);
        self::assertEquals('tableValue', $table['metadata'][0]['value']);
        self::assertEquals('columnKey', $table['columnMetadata']['Id'][0]['key']);
        self::assertEquals('columnValue', $table['columnMetadata']['Id'][0]['value']);

        $bucket = $this->sapiClient->listBuckets(['include' => 'metadata'])[0];
        self::assertEquals('bucketKey', $bucket['metadata'][0]['key']);
        self::assertEquals('bucketValue', $bucket['metadata'][0]['value']);
    }

    public function testRestoreTransformationMetadata(): void
    {
        $backup = new S3Restore(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            'transformation-with-metadata'
        );
        $backup->restoreConfigs();

        $components = new Components($this->branchAwareClient);

        $options = new ListConfigurationMetadataOptions();
        $options->setComponentId('keboola.snowflake-transformation');
        $options->setConfigurationId('sapi-php-test');

        $metadata = $components->listConfigurationMetadata($options);

        Assert::assertEquals('KBC.configuration.folderName', $metadata[0]['key']);
        Assert::assertEquals('testFolder', $metadata[0]['value']);
    }
}
