<?php

declare(strict_types=1);

namespace Keboola\ProjectRestore\Tests;

use Aws\S3\S3Client;
use Keboola\StorageApi\Client as StorageApi;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Exception;
use Keboola\ProjectRestore\S3Restore;
use Keboola\StorageApi\TableExporter;
use Keboola\Temp\Temp;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;

class S3RestoreTest extends TestCase
{
    /**
     * @var StorageApi
     */
    private $sapiClient;

    /**
     * @var S3Client
     */
    private $s3Client;

    public function setUp(): void
    {
        parent::setUp();

        $this->sapiClient = new StorageApi([
            'url' => getenv('TEST_STORAGE_API_URL'),
            'token' => getenv('TEST_STORAGE_API_TOKEN'),
        ]);

        $this->cleanupKbcProject();

        putenv('AWS_ACCESS_KEY_ID=' . getenv('TEST_AWS_ACCESS_KEY_ID'));
        putenv('AWS_SECRET_ACCESS_KEY=' . getenv('TEST_AWS_SECRET_ACCESS_KEY'));

        $this->s3Client = new S3Client([
            'version' => 'latest',
            'region' => getenv('TEST_AWS_REGION'),
        ]);
    }

    public function testRestoreBuckets(): void
    {
        $backup = new S3Restore($this->s3Client, $this->sapiClient);
        $backup->restoreBuckets(getenv('TEST_AWS_S3_BUCKET'), 'buckets', true);

        $buckets = $this->sapiClient->listBuckets();
        self::assertCount(2, $buckets);
        self::assertEquals("in.c-bucket1", $buckets[0]["id"]);
        self::assertEquals("in.c-bucket2", $buckets[1]["id"]);
    }

    public function testRestoreLinkedBuckets(): void
    {
        $backup = new S3Restore($this->s3Client, $this->sapiClient);
        $backup->restoreBuckets(getenv('TEST_AWS_S3_BUCKET'), 'buckets-linked-bucket', true);
        $backup->restoreTables(getenv('TEST_AWS_S3_BUCKET'), 'buckets-linked-bucket');
        $backup->restoreTableAliases(getenv('TEST_AWS_S3_BUCKET'), 'buckets-linked-bucket');

        $buckets = $this->sapiClient->listBuckets();
        self::assertCount(2, $buckets);
        self::assertEquals("in.c-bucket1", $buckets[0]["id"]);
        self::assertEquals("in.c-bucket2", $buckets[1]["id"]);

        $tables = $this->sapiClient->listTables($buckets[0]["id"]);
        self::assertCount(1, $tables);
        self::assertEquals("in.c-bucket1.sample", $tables[0]["id"]);
    }

    public function testRestoreBucketsIgnoreStorageBackend(): void
    {
        $backup = new S3Restore($this->s3Client, $this->sapiClient);
        $backup->restoreBuckets(getenv('TEST_AWS_S3_BUCKET'), 'buckets-multiple-backends', false);

        $buckets = $this->sapiClient->listBuckets();
        self::assertCount(3, $buckets);
        self::assertTrue($this->sapiClient->bucketExists("in.c-snowflake"));
        self::assertTrue($this->sapiClient->bucketExists("in.c-redshift"));
        self::assertTrue($this->sapiClient->bucketExists("in.c-mysql"));
    }

    public function testBackendMissingError(): void
    {
        $backup = new S3Restore($this->s3Client, $this->sapiClient);

        try {
            $backup->restoreBuckets(getenv('TEST_AWS_S3_BUCKET'), 'buckets-multiple-backends', true);
            self::fail('Restoring buckets with non-supported backends should fail');
        } catch (Exception $e) {
            self::assertContains('Missing', $e->getMessage());
            self::assertContains('backend', $e->getMessage());
        }
    }

    public function testRestoreBucketAttributes(): void
    {
        $backup = new S3Restore($this->s3Client, $this->sapiClient);
        $backup->restoreBuckets(getenv('TEST_AWS_S3_BUCKET'), 'buckets', true);

        self::assertEquals(
            [
                [
                    "name" => "myKey",
                    "value" => "myValue",
                    "protected" => false,
                ],
                [
                    "name" => "myProtectedKey",
                    "value" => "myProtectedValue",
                    "protected" => true,
                ],
            ],
            $this->sapiClient->getBucket("in.c-bucket1")["attributes"]
        );
    }

    public function testRestoreTableWithHeader(): void
    {
        $backup = new S3Restore($this->s3Client, $this->sapiClient);
        $backup->restoreBuckets(getenv('TEST_AWS_S3_BUCKET'), 'table-with-header', true);
        $backup->restoreTables(getenv('TEST_AWS_S3_BUCKET'), 'table-with-header');

        $temp = new Temp();
        $temp->initRunFolder();

        self::assertTrue($this->sapiClient->tableExists("in.c-bucket.Account"));

        $tableExporter = new TableExporter($this->sapiClient);
        $file = $temp->createFile("account.csv");
        $tableExporter->exportTable("in.c-bucket.Account", $file->getPathname(), []);
        $fileContents = file_get_contents($file->getPathname());
        self::assertContains('"Id","Name"', $fileContents);
        self::assertContains('"001C000000xYbhhIAC","Keboola"', $fileContents);
        self::assertContains('"001C000000xYbhhIAD","Keboola 2"', $fileContents);
    }

    public function testRestoreTableWithoutHeader(): void
    {
        $backup = new S3Restore($this->s3Client, $this->sapiClient);
        $backup->restoreBuckets(getenv('TEST_AWS_S3_BUCKET'), 'table-without-header', true);
        $backup->restoreTables(getenv('TEST_AWS_S3_BUCKET'), 'table-without-header');

        $temp = new Temp();
        $temp->initRunFolder();

        self::assertTrue($this->sapiClient->tableExists("in.c-bucket.Account"));

        $tableExporter = new TableExporter($this->sapiClient);
        $file = $temp->createFile("account.csv");
        $tableExporter->exportTable("in.c-bucket.Account", $file->getPathname(), []);
        $fileContents = file_get_contents($file->getPathname());
        self::assertContains('"Id","Name"', $fileContents);
        self::assertContains('"001C000000xYbhhIAC","Keboola"', $fileContents);
        self::assertContains('"001C000000xYbhhIAD","Keboola 2"', $fileContents);
    }

    public function testRestoreTableFromMultipleSlices(): void
    {
        $backup = new S3Restore($this->s3Client, $this->sapiClient);
        $backup->restoreBuckets(getenv('TEST_AWS_S3_BUCKET'), 'table-multiple-slices', true);
        $backup->restoreTables(getenv('TEST_AWS_S3_BUCKET'), 'table-multiple-slices');

        $temp = new Temp();
        $temp->initRunFolder();

        self::assertTrue($this->sapiClient->tableExists("in.c-bucket.Account"));
        $tableExporter = new TableExporter($this->sapiClient);
        $file = $temp->createFile("account.csv");
        $tableExporter->exportTable("in.c-bucket.Account", $file->getPathname(), []);
        $fileContents = file_get_contents($file->getPathname());
        self::assertContains('"Id","Name"', $fileContents);
        self::assertContains('"001C000000xYbhhIAC","Keboola"', $fileContents);
        self::assertContains('"001C000000xYbhhIAD","Keboola 2"', $fileContents);
    }

    public function testRestoreTableFromMultipleSlicesSharedPrefix(): void
    {
        $backup = new S3Restore($this->s3Client, $this->sapiClient);
        $backup->restoreBuckets(getenv('TEST_AWS_S3_BUCKET'), 'table-multiple-slices-shared-prefix', true);
        $backup->restoreTables(getenv('TEST_AWS_S3_BUCKET'), 'table-multiple-slices-shared-prefix');

        $temp = new Temp();
        $temp->initRunFolder();

        self::assertTrue($this->sapiClient->tableExists("in.c-bucket.Account"));
        self::assertTrue($this->sapiClient->tableExists("in.c-bucket.Account2"));

        $tableExporter = new TableExporter($this->sapiClient);
        $file = $temp->createFile("account.csv");
        $tableExporter->exportTable("in.c-bucket.Account", $file->getPathname(), []);
        $fileContents = file_get_contents($file->getPathname());
        self::assertContains('"Id","Name"', $fileContents);
        self::assertContains('"001C000000xYbhhIAC","Keboola"', $fileContents);
        self::assertContains('"001C000000xYbhhIAD","Keboola 2"', $fileContents);
        self::assertCount(4, explode("\n", $fileContents));

        $file = $temp->createFile("account2.csv");
        $tableExporter->exportTable("in.c-bucket.Account2", $file->getPathname(), []);
        $fileContents = file_get_contents($file->getPathname());
        self::assertContains('"Id","Name"', $fileContents);
        self::assertContains('"001C000000xYbhhIAC","Keboola"', $fileContents);
        self::assertContains('"001C000000xYbhhIAD","Keboola 2"', $fileContents);
        self::assertCount(4, explode("\n", $fileContents));
    }

    public function testRestoreTableAttributes(): void
    {
        $backup = new S3Restore($this->s3Client, $this->sapiClient);
        $backup->restoreBuckets(getenv('TEST_AWS_S3_BUCKET'), 'table-properties', true);
        $backup->restoreTables(getenv('TEST_AWS_S3_BUCKET'), 'table-properties');

        self::assertEquals(
            [
                [
                    "name" => "myKey",
                    "value" => "myValue",
                    "protected" => false,
                ],
                [
                    "name" => "myProtectedKey",
                    "value" => "myProtectedValue",
                    "protected" => true,
                ],
            ],
            $this->sapiClient->getTable("in.c-bucket.Account")["attributes"]
        );
    }

    public function testRestoreTablePrimaryKeys(): void
    {
        $backup = new S3Restore($this->s3Client, $this->sapiClient);
        $backup->restoreBuckets(getenv('TEST_AWS_S3_BUCKET'), 'table-properties', true);
        $backup->restoreTables(getenv('TEST_AWS_S3_BUCKET'), 'table-properties');

        $accountTable = $this->sapiClient->getTable("in.c-bucket.Account");
        $account2Table = $this->sapiClient->getTable("in.c-bucket.Account2");
        self::assertEquals(["Id", "Name"], $accountTable["primaryKey"]);
        self::assertEquals(["Id"], $account2Table["primaryKey"]);
    }

    public function testRestoreAlias(): void
    {
        $backup = new S3Restore($this->s3Client, $this->sapiClient);
        $backup->restoreBuckets(getenv('TEST_AWS_S3_BUCKET'), 'alias', true);
        $backup->restoreTables(getenv('TEST_AWS_S3_BUCKET'), 'alias');
        $backup->restoreTableAliases(getenv('TEST_AWS_S3_BUCKET'), 'alias');

        $aliasTable = $this->sapiClient->getTable("out.c-bucket.Account");
        self::assertEquals(true, $aliasTable["isAlias"]);
        self::assertEquals(true, $aliasTable["aliasColumnsAutoSync"]);
        self::assertEquals(["Id", "Name"], $aliasTable["columns"]);
        self::assertEquals("in.c-bucket.Account", $aliasTable["sourceTable"]["id"]);
    }

    public function testRestoreAliasAttributes(): void
    {
        $backup = new S3Restore($this->s3Client, $this->sapiClient);
        $backup->restoreBuckets(getenv('TEST_AWS_S3_BUCKET'), 'alias-properties', true);
        $backup->restoreTables(getenv('TEST_AWS_S3_BUCKET'), 'alias-properties');
        $backup->restoreTableAliases(getenv('TEST_AWS_S3_BUCKET'), 'alias-properties');

        self::assertEquals(
            [
                [
                    "name" => "myKey",
                    "value" => "myValue",
                    "protected" => false,
                ],
                [
                    "name" => "myProtectedKey",
                    "value" => "myProtectedValue",
                    "protected" => true,
                ],
            ],
            $this->sapiClient->getTable("out.c-bucket.Account")["attributes"]
        );
    }

    public function testRestoreAliasMetadata(): void
    {
        $backup = new S3Restore($this->s3Client, $this->sapiClient);
        $backup->restoreBuckets(getenv('TEST_AWS_S3_BUCKET'), 'alias-metadata', true);
        $backup->restoreTables(getenv('TEST_AWS_S3_BUCKET'), 'alias-metadata');
        $backup->restoreTableAliases(getenv('TEST_AWS_S3_BUCKET'), 'alias-metadata');

        self::assertTrue($this->sapiClient->tableExists("out.c-bucket.Account"));

        $aliasTable = $this->sapiClient->getTable("out.c-bucket.Account");
        self::assertEquals("tableKey", $aliasTable["metadata"][0]["key"]);
        self::assertEquals("tableValue", $aliasTable["metadata"][0]["value"]);
        self::assertEquals("columnKey", $aliasTable["columnMetadata"]["Id"][0]["key"]);
        self::assertEquals("columnValue", $aliasTable["columnMetadata"]["Id"][0]["value"]);
    }

    public function testRestoreFilteredAlias(): void
    {
        $backup = new S3Restore($this->s3Client, $this->sapiClient);
        $backup->restoreBuckets(getenv('TEST_AWS_S3_BUCKET'), 'alias-filtered', true);
        $backup->restoreTables(getenv('TEST_AWS_S3_BUCKET'), 'alias-filtered');
        $backup->restoreTableAliases(getenv('TEST_AWS_S3_BUCKET'), 'alias-filtered');

        $aliasTable = $this->sapiClient->getTable("out.c-bucket.Account");
        self::assertEquals(true, $aliasTable["isAlias"]);
        self::assertEquals(false, $aliasTable["aliasColumnsAutoSync"]);
        self::assertEquals(["Id"], $aliasTable["columns"]);
        self::assertEquals("in.c-bucket.Account", $aliasTable["sourceTable"]["id"]);
        self::assertEquals(["column" => "Name", "operator" => "eq", "values" => ["Keboola"]], $aliasTable["aliasFilter"]);
    }

    public function testRestoreConfigurations(): void
    {
        $backup = new S3Restore($this->s3Client, $this->sapiClient);
        $backup->restoreConfigs(getenv('TEST_AWS_S3_BUCKET'), 'configurations');

        $components = new Components($this->sapiClient);
        $componentsList = $components->listComponents();

        self::assertCount(2, $componentsList);
        self::assertEquals("keboola.csv-import", $componentsList[0]["id"]);
        self::assertEquals("keboola.ex-slack", $componentsList[1]["id"]);

        $config = $components->getConfiguration("keboola.csv-import", 1);
        self::assertEquals(1, $config["version"]);
        self::assertEquals("Configuration created", $config["changeDescription"]);
        self::assertEquals("Accounts", $config["name"]);
        self::assertEquals("Default CSV Importer", $config["description"]);
        self::assertEquals(["key" => "value"], $config["state"]);

        $config = $components->getConfiguration("keboola.ex-slack", 2);
        self::assertEquals(2, $config["version"]);
        self::assertEquals("Configuration 2 restored from backup", $config["changeDescription"]);
        self::assertEmpty($config["state"]);
    }

    public function testRestoreConfigurationsWithoutVersions(): void
    {
        $backup = new S3Restore($this->s3Client, $this->sapiClient);
        $backup->restoreConfigs(getenv('TEST_AWS_S3_BUCKET'), 'configurations-no-versions');

        $components = new Components($this->sapiClient);
        $componentsList = $components->listComponents();

        self::assertCount(2, $componentsList);
        self::assertEquals("keboola.csv-import", $componentsList[0]["id"]);
        self::assertEquals("keboola.ex-slack", $componentsList[1]["id"]);

        $config = $components->getConfiguration("keboola.csv-import", 1);

        self::assertEquals(1, $config["version"]);
        self::assertEquals("Configuration created", $config["changeDescription"]);
        self::assertEquals("Accounts", $config["name"]);
        self::assertEquals("Default CSV Importer", $config["description"]);
        self::assertEquals(["key" => "value"], $config["state"]);

        $config = $components->getConfiguration("keboola.ex-slack", 2);
        self::assertEquals(2, $config["version"]);
        self::assertEquals("Configuration 2 restored from backup", $config["changeDescription"]);
        self::assertEmpty($config["state"]);
    }

    public function testDoNotRestoreObsoleteConfigurations(): void
    {
        $backup = new S3Restore($this->s3Client, $this->sapiClient);
        $backup->restoreConfigs(getenv('TEST_AWS_S3_BUCKET'), 'configuration-obsolete');

        $components = new Components($this->sapiClient);
        $componentsList = $components->listComponents();

        self::assertCount(1, $componentsList);
        self::assertEquals("keboola.csv-import", $componentsList[0]["id"]);
        self::assertCount(1, $componentsList[0]["configurations"]);
    }

    public function testRestoreEmptyObjectInConfiguration(): void
    {
        $backup = new S3Restore($this->s3Client, $this->sapiClient);
        $backup->restoreConfigs(getenv('TEST_AWS_S3_BUCKET'), 'configuration-empty-object');

        $temp = new Temp();
        $temp->initRunFolder();

        // empty array and object in config
        $file = $temp->createFile('config.json');
        $this->sapiClient->apiGet('storage/components/keboola.csv-import/configs/1', $file->getPathname());
        $config = json_decode(file_get_contents($file->getPathname()));
        self::assertEquals(new \stdClass(), $config->configuration->emptyObject);
        self::assertEquals([], $config->configuration->emptyArray);
    }

    public function testRestoreConfigurationRows(): void
    {
        $backup = new S3Restore($this->s3Client, $this->sapiClient);
        $backup->restoreConfigs(getenv('TEST_AWS_S3_BUCKET'), 'configuration-rows');

        $components = new Components($this->sapiClient);
        $componentsList = $components->listComponents();

        self::assertCount(1, $componentsList);
        self::assertEquals("transformation", $componentsList[0]["id"]);
        self::assertCount(2, $componentsList[0]["configurations"]);

        $config = $components->getConfiguration("transformation", 1);
        self::assertEquals("MySQL", $config["name"]);
        self::assertEquals(5, $config["version"]);
        self::assertEquals("Row 4 restored from backup", $config["changeDescription"]);
        self::assertCount(2, $config["rows"]);
        self::assertEquals(3, $config["rows"][0]["id"]);
        self::assertEquals("Account", $config["rows"][0]["configuration"]["name"]);
        self::assertEquals(["rowKey" => "value"], $config["rows"][0]["state"]);
        self::assertEquals(4, $config["rows"][1]["id"]);
        self::assertEquals("Ratings", $config["rows"][1]["configuration"]["name"]);
        self::assertEmpty($config["rows"][1]["state"]);

        $config = $components->getConfiguration("transformation", 2);
        self::assertEquals("Snowflake", $config["name"]);
        self::assertEquals(5, $config["version"]);
        self::assertEquals("Row 6 restored from backup", $config["changeDescription"]);
        self::assertEquals(5, $config["rows"][0]["id"]);
        self::assertEquals("Account", $config["rows"][0]["configuration"]["name"]);
        self::assertEmpty($config["rows"][0]["state"]);
        self::assertEquals(6, $config["rows"][1]["id"]);
        self::assertEquals("Ratings", $config["rows"][1]["configuration"]["name"]);
        self::assertEmpty($config["rows"][1]["state"]);
    }

    public function testRestoreEmptyObjectInConfigurationRow(): void
    {
        $backup = new S3Restore($this->s3Client, $this->sapiClient);
        $backup->restoreConfigs(getenv('TEST_AWS_S3_BUCKET'), 'configuration-rows');

        $temp = new Temp();
        $temp->initRunFolder();

        // empty array and object in config rows
        $file = $temp->createFile('config.json');
        $this->sapiClient->apiGet('storage/components/transformation/configs/1/rows', $file->getPathname());
        $config = json_decode(file_get_contents($file->getPathname()));
        self::assertEquals(new \stdClass(), $config[0]->configuration->input[0]->datatypes);
        self::assertEquals([], $config[0]->configuration->queries);
    }

    public function testRestoreBucketWithoutPrefix(): void
    {
        $backup = new S3Restore($this->s3Client, $this->sapiClient);
        $backup->restoreBuckets(getenv('TEST_AWS_S3_BUCKET'), 'bucket-without-prefix', true);

        $buckets = $this->sapiClient->listBuckets();
        self::assertCount(0, $buckets);
    }

    public function testRestoreTableWithoutPrefix(): void
    {
        $backup = new S3Restore($this->s3Client, $this->sapiClient);
        $backup->restoreBuckets(getenv('TEST_AWS_S3_BUCKET'), 'table-without-prefix', true);
        $backup->restoreTables(getenv('TEST_AWS_S3_BUCKET'), 'table-without-prefix');

        $buckets = $this->sapiClient->listBuckets();
        self::assertCount(0, $buckets);
    }

    public function testRestoreTableEmpty(): void
    {
        $backup = new S3Restore($this->s3Client, $this->sapiClient);
        $backup->restoreBuckets(getenv('TEST_AWS_S3_BUCKET'), 'table-empty', true);
        $backup->restoreTables(getenv('TEST_AWS_S3_BUCKET'), 'table-empty');

        self::assertTrue($this->sapiClient->tableExists("in.c-bucket.Account"));
    }

    public function testRestoreMetadata(): void
    {
        $backup = new S3Restore($this->s3Client, $this->sapiClient);
        $backup->restoreBuckets(getenv('TEST_AWS_S3_BUCKET'), 'metadata', true);
        $backup->restoreTables(getenv('TEST_AWS_S3_BUCKET'), 'metadata');

        self::assertTrue($this->sapiClient->tableExists("in.c-bucket.Account"));

        $table = $this->sapiClient->getTable("in.c-bucket.Account");

        self::assertEquals("tableKey", $table["metadata"][0]["key"]);
        self::assertEquals("tableValue", $table["metadata"][0]["value"]);
        self::assertEquals("columnKey", $table["columnMetadata"]["Id"][0]["key"]);
        self::assertEquals("columnValue", $table["columnMetadata"]["Id"][0]["value"]);

        $bucket = $this->sapiClient->listBuckets(["include" => "metadata"])[0];
        self::assertEquals("bucketKey", $bucket["metadata"][0]["key"]);
        self::assertEquals("bucketValue", $bucket["metadata"][0]["value"]);
    }

    private function cleanupKbcProject(): void
    {
        $components = new Components($this->sapiClient);
        foreach ($components->listComponents() as $component) {
            foreach ($component['configurations'] as $configuration) {
                $components->deleteConfiguration($component['id'], $configuration['id']);

                // delete configuration from trash
                $components->deleteConfiguration($component['id'], $configuration['id']);
            }
        }

        // drop linked buckets
        foreach ($this->sapiClient->listBuckets() as $bucket) {
            if (isset($bucket['sourceBucket'])) {
                $this->sapiClient->dropBucket($bucket["id"], ["force" => true]);
            }
        }

        foreach ($this->sapiClient->listBuckets() as $bucket) {
            $this->sapiClient->dropBucket($bucket["id"], ["force" => true]);
        }
    }
}
