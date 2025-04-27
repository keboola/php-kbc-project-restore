<?php

declare(strict_types=1);

namespace Keboola\ProjectRestore\Tests;

use Keboola\ProjectRestore\Restore;
use Keboola\ProjectRestore\StorageApi\BucketInfo;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\DevBranchesMetadata;
use Keboola\StorageApi\Exception;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\TableExporter;
use Keboola\Temp\Temp;
use PHPUnit\Framework\Assert;
use Psr\Log\LoggerInterface;
use Psr\Log\Test\TestLogger;

abstract class AbstractRestoreTestPart1 extends BaseTest
{
    public const TEST_ITERATOR_SLICES_COUNT = 120;

    abstract protected function createRestoreInstance(string $postfix, ?LoggerInterface $logger = null): Restore;

    public function testProjectMetadataRestore(): void
    {
        $metadata = new DevBranchesMetadata($this->branchAwareClient);
        $metadataList = $metadata->listBranchMetadata();
        foreach ($metadataList as $item) {
            $metadata->deleteBranchMetadata((int) $item['id']);
        }

        $restore = $this->createRestoreInstance('branches-metadata');
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

        $restore = $this->createRestoreInstance('branches-empty-metadata');
        $restore->restoreProjectMetadata();

        $metadataList = $metadata->listBranchMetadata();
        self::assertEquals(0, count($metadataList));
    }

    public function testBucketsInBackup(): void
    {
        $restore = $this->createRestoreInstance('buckets');
        $buckets = $restore->getBucketsInBackup();

        self::assertCount(2, $buckets);

        foreach ($buckets as $bucketInfo) {
            $this->assertInstanceOf(BucketInfo::class, $bucketInfo);
        }

        self::assertEquals('in.c-bucket1', $buckets[0]->getId());
        self::assertEquals('in.c-bucket2', $buckets[1]->getId());
    }

    public function testBucketRestore(): void
    {
        $restore = $this->createRestoreInstance('buckets');
        $buckets = $restore->getBucketsInBackup();
        foreach ($buckets as $bucketInfo) {
            $restore->restoreBucket($bucketInfo);
        }

        $buckets = $this->sapiClient->listBuckets();
        self::assertCount(2, $buckets);
        self::assertEquals('in.c-bucket1', $buckets[0]['id']);
        self::assertEquals('in.c-bucket2', $buckets[1]['id']);
    }

    public function testBucketMetadataRestore(): void
    {
        $restore = $this->createRestoreInstance('metadata');
        $buckets = $restore->getBucketsInBackup();
        foreach ($buckets as $bucketInfo) {
            $restore->restoreBucket($bucketInfo);
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
        $restore = $this->createRestoreInstance('buckets-multiple-backends');
        $buckets = $restore->getBucketsInBackup();
        foreach ($buckets as $bucketInfo) {
            $restore->restoreBucket($bucketInfo, true);
        }

        $buckets = $this->sapiClient->listBuckets();
        self::assertCount(2, $buckets);
        self::assertTrue($this->sapiClient->bucketExists('in.c-snowflake'));
        self::assertTrue($this->sapiClient->bucketExists('in.c-redshift'));
    }

    public function testBucketMissingBackend(): void
    {
        $restore = $this->createRestoreInstance('buckets-multiple-backends');

        $tokenData = $this->sapiClient->verifyToken();
        $projectData = $tokenData['owner'];

        $buckets = $restore->getBucketsInBackup();
        self::assertCount(2, $buckets);

        $fails = 0;
        foreach ($buckets as $bucketInfo) {
            if ($bucketInfo->getBackend() === $projectData['defaultBackend']) {
                continue;
            }

            try {
                $restore->restoreBucket($bucketInfo);
                self::fail('Restoring bucket with non-supported backend should fail');
            } catch (ClientException $e) {
                $message1 = 'is not supported for project';
                $message2 = 'was not found in the haystack';

                self::assertTrue(
                    strpos($e->getMessage(), $message1) !== false
                    || strpos($e->getMessage(), $message2) !== false,
                );
                $fails++;
            }
        }

        self::assertGreaterThan(0, $fails);
    }

    public function testBucketWithoutPrefixRestore(): void
    {
        $restore = $this->createRestoreInstance('bucket-without-prefix');
        $buckets = $restore->getBucketsInBackup();

        try {
            $restore->restoreBucket(reset($buckets));
            self::fail('Restoring bucket with non-supported backend should fail');
        } catch (Exception $e) {
            self::assertStringContainsString('System bucket restore is not supported', $e->getMessage());
        }
    }

    public function testBucketLinkRestore(): void
    {
        $restore = $this->createRestoreInstance('buckets-linked-bucket');
        $buckets = $restore->getBucketsInBackup();

        $fails = 0;
        foreach ($buckets as $bucketInfo) {
            if (!$bucketInfo->isLinkedBucket()) {
                continue;
            }

            try {
                $restore->restoreBucket($bucketInfo);
                self::fail('Restoring bucket with non-supported backend should fail');
            } catch (Exception $e) {
                self::assertStringContainsString('Linked bucket restore is not supported', $e->getMessage());
                $fails++;
            }
        }

        self::assertGreaterThan(0, $fails);
    }

    public function testRestoreBuckets(): void
    {
        $restore = $this->createRestoreInstance('buckets');
        $restore->restoreBuckets(true);

        $buckets = $this->sapiClient->listBuckets();
        $metadata = new Metadata($this->sapiClient);
        $bucketMetadata = $metadata->listBucketMetadata('in.c-bucket1');

        self::assertCount(2, $buckets);
        self::assertCount(1, $bucketMetadata);
        self::assertEquals('in.c-bucket1', $buckets[0]['id']);
        self::assertEquals('in.c-bucket2', $buckets[1]['id']);
        self::assertEquals('some-key', $bucketMetadata[0]['key']);
        self::assertEquals('Some value', $bucketMetadata[0]['value']);
        self::assertEquals('test-component', $bucketMetadata[0]['provider']);
    }

    public function testRestoreLinkedBuckets(): void
    {
        $restore = $this->createRestoreInstance('buckets-linked-bucket');
        $restore->restoreBuckets(true);
        $restore->restoreTables();
        $restore->restoreTableAliases();

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
        $restore = $this->createRestoreInstance('buckets-multiple-backends');
        $restore->restoreBuckets(false);

        $buckets = $this->sapiClient->listBuckets();
        self::assertCount(2, $buckets);
        self::assertTrue($this->sapiClient->bucketExists('in.c-snowflake'));
        self::assertTrue($this->sapiClient->bucketExists('in.c-redshift'));
    }

    public function testBackendMissingError(): void
    {
        $restore = $this->createRestoreInstance('buckets-multiple-backends');

        try {
            $restore->restoreBuckets(true);
            self::fail('Restoring buckets with non-supported backends should fail');
        } catch (Exception $e) {
            self::assertStringContainsString('Missing', $e->getMessage());
            self::assertStringContainsString('backend', $e->getMessage());
        }
    }

    public function testRestoreTableWithHeader(): void
    {
        $restore = $this->createRestoreInstance('table-with-header');
        $restore->restoreBuckets(true);
        $restore->restoreTables();

        $temp = new Temp();

        self::assertTrue($this->sapiClient->tableExists('in.c-bucket.Account'));

        $tableExporter = new TableExporter($this->sapiClient);
        $file = $temp->createFile('account.csv');
        $tableExporter->exportTable('in.c-bucket.Account', $file->getPathname(), []);
        $fileContents = (string) file_get_contents($file->getPathname());
        self::assertStringContainsString('"Id","Name"', $fileContents);
        self::assertStringContainsString('"001C000000xYbhhIAC","Keboola"', $fileContents);
        self::assertStringContainsString('"001C000000xYbhhIAD","Keboola 2"', $fileContents);
    }

    public function testRestoreTableWithoutHeader(): void
    {
        $restore = $this->createRestoreInstance('table-without-header');
        $restore->restoreBuckets(true);
        $restore->restoreTables();

        $temp = new Temp();

        self::assertTrue($this->sapiClient->tableExists('in.c-bucket.Account'));

        $tableExporter = new TableExporter($this->sapiClient);
        $file = $temp->createFile('account.csv');
        $tableExporter->exportTable('in.c-bucket.Account', $file->getPathname(), []);
        $fileContents = (string) file_get_contents($file->getPathname());
        self::assertStringContainsString('"Id","Name"', $fileContents);
        self::assertStringContainsString('"001C000000xYbhhIAC","Keboola"', $fileContents);
        self::assertStringContainsString('"001C000000xYbhhIAD","Keboola 2"', $fileContents);
    }

    public function testRestoreTableFromMultipleSlices(): void
    {
        $restore = $this->createRestoreInstance('table-multiple-slices');
        $restore->restoreBuckets(true);
        $restore->restoreTables();

        $temp = new Temp();

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
        $restore = $this->createRestoreInstance($sourceBucket);

        $restore->restoreBuckets(true);
        $restore->restoreTables();

        $table = $this->sapiClient->getTable('in.c-bucket.Account');
        $this->assertEquals(self::TEST_ITERATOR_SLICES_COUNT, $table['rowsCount']);
    }

    public function testRestoreTableFromMultipleSlicesSharedPrefix(): void
    {
        $restore = $this->createRestoreInstance('table-multiple-slices-shared-prefix');
        $restore->restoreBuckets(true);
        $restore->restoreTables();

        $temp = new Temp();

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
        $restore = $this->createRestoreInstance('table-properties');
        $restore->restoreBuckets(true);
        $restore->restoreTables();

        $accountTable = $this->sapiClient->getTable('in.c-bucket.Account');
        $account2Table = $this->sapiClient->getTable('in.c-bucket.Account2');
        self::assertEquals(['Id', 'Name'], $accountTable['primaryKey']);
        self::assertEquals(['Id'], $account2Table['primaryKey']);
    }

    public function testRestoreNativeDataTypesTable(): void
    {
        $restore = $this->createRestoreInstance('native-data-types-table');
        $restore->restoreBuckets(true);
        $restore->restoreTables();

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
                'KBC.datatype.basetype' => 'INTEGER',
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
                Assert::assertArrayHasKey($key, $expectedMetadata[$provider][$column], $column);
                Assert::assertEquals($expectedMetadata[$provider][$column][$key], $columnMetadata['value'], $column);
                $countSavedMetadata++;
            }
        }
        Assert::assertEquals(18, $countSavedMetadata);
    }

    public function testRestoreAlias(): void
    {
        $restore = $this->createRestoreInstance('alias');
        $restore->restoreBuckets(true);
        $restore->restoreTables();
        $restore->restoreTableAliases();

        $aliasTable = $this->sapiClient->getTable('out.c-bucket.Account');
        self::assertEquals(true, $aliasTable['isAlias']);
        self::assertEquals(true, $aliasTable['aliasColumnsAutoSync']);
        self::assertEquals(['Id', 'Name'], $aliasTable['columns']);
        self::assertEquals('in.c-bucket.Account', $aliasTable['sourceTable']['id']);
    }

    public function testRestoreAliasMetadata(): void
    {
        $restore = $this->createRestoreInstance('alias-metadata');
        $restore->restoreBuckets();
        $restore->restoreTables();
        $restore->restoreTableAliases();

        self::assertTrue($this->sapiClient->tableExists('out.c-bucket.Account'));

        $aliasTable = $this->sapiClient->getTable('out.c-bucket.Account');
        self::assertEquals('tableKey', $aliasTable['metadata'][0]['key']);
        self::assertEquals('tableValue', $aliasTable['metadata'][0]['value']);
        self::assertEquals('columnKey', $aliasTable['columnMetadata']['Id'][0]['key']);
        self::assertEquals('columnValue', $aliasTable['columnMetadata']['Id'][0]['value']);
    }

    public function testRestoreFilteredAlias(): void
    {
        $restore = $this->createRestoreInstance('alias-filtered');
        $restore->restoreBuckets();
        $restore->restoreTables();
        $restore->restoreTableAliases();

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
            $aliasTable['aliasFilter'],
        );
    }

    public function testRestoreBucketWithoutPrefix(): void
    {
        $restore = $this->createRestoreInstance('bucket-without-prefix');
        $restore->restoreBuckets(true);

        $buckets = $this->sapiClient->listBuckets();
        self::assertCount(0, $buckets);
    }

    public function testRestoreTableWithoutPrefix(): void
    {
        $restore = $this->createRestoreInstance('table-without-prefix');
        $restore->restoreBuckets(true);
        $restore->restoreTables();

        $buckets = $this->sapiClient->listBuckets();
        self::assertCount(0, $buckets);
    }

    public function testRestoreTableEmpty(): void
    {
        $restore = $this->createRestoreInstance('table-empty');
        $restore->restoreBuckets();
        $restore->restoreTables();

        self::assertTrue($this->sapiClient->tableExists('in.c-bucket.Account'));
    }

    public function testRestoreMetadata(): void
    {
        $restore = $this->createRestoreInstance('metadata');
        $restore->restoreBuckets(true);
        $restore->restoreTables();

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

    public function testRestoreAliasWithSourceTableDoesntExists(): void
    {
        $testLogger = new TestLogger();
        $restore = $this->createRestoreInstance('alias-source-table-doesnt-exists', $testLogger);
        $restore->restoreTableAliases();

        self::assertTrue($testLogger->hasWarning(
            'Skipping alias out.c-bucket.Account - ' .
            'source table with id "in.c-bucket-doesnt-exists.tables-doesnt-exists" does not exist',
        ));
    }

    public function testRestoreTypedTableWithIntColumns(): void
    {
        $restore = $this->createRestoreInstance('typed-table-with-int-columns');

        $restore->restoreBuckets();
        $restore->restoreTables();

        self::assertTrue($this->sapiClient->tableExists('in.c-bucket.firstTable'));

        $table = $this->sapiClient->getTable('in.c-bucket.firstTable');
        self::assertTrue($table['isTyped']);
        self::assertEquals(['1'], $table['definition']['primaryKeysNames']);

        $columnNames = array_map(
            function ($column) {
                return $column['name'];
            },
            $table['definition']['columns'],
        );
        self::assertEquals(['1', '2', '3', '4'], $columnNames);
    }

    public function testRestoreTableWithNullablePKs(): void
    {
        $logger = new TestLogger();
        $restore = $this->createRestoreInstance('table-with-nullable-pk', $logger);
        $restore->setDryRunMode();

        $restore->restoreBuckets();
        $restore->restoreTables();

        self::assertTrue($logger->hasWarning(
            'Table "firstTable" cannot be restored because the primary key column "Id" is nullable.',
        ));
    }

    public function testRestoreTableWithDisplayName(): void
    {
        $restore = $this->createRestoreInstance('table-with-display-name');
        $restore->restoreBuckets();
        $restore->restoreTables();

        $firstTable = $this->sapiClient->getTable('in.c-bucket.firstTable');
        self::assertEquals('DisplayNameFirstTable', $firstTable['displayName']);

        $secondTable = $this->sapiClient->getTable('in.c-bucket.secondTable');
        self::assertEquals('DisplayNameSecondTable', $secondTable['displayName']);
    }
}
