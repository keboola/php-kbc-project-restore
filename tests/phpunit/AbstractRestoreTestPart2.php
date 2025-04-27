<?php

declare(strict_types=1);

namespace Keboola\ProjectRestore\Tests;

use Keboola\ProjectRestore\Restore;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\ListConfigurationMetadataOptions;
use Keboola\StorageApi\DevBranchesMetadata;
use Keboola\StorageApi\Metadata;
use Keboola\Temp\Temp;
use PHPUnit\Framework\Assert;
use Psr\Log\LoggerInterface;
use Psr\Log\Test\TestLogger;
use stdClass;

abstract class AbstractRestoreTestPart2 extends BaseTest
{
    abstract protected function createRestoreInstance(string $postfix, ?LoggerInterface $logger = null): Restore;

    public function testPermanentFilesRestore(): void
    {
        $files = $this->sapiClient->listFiles();
        foreach ($files as $file) {
            $this->sapiClient->deleteFile($file['id']);
        }
        $restore = $this->createRestoreInstance('permanent-files');
        $restore->restorePermanentFiles();
        sleep(1);

        sleep(3);

        $files = $this->sapiClient->listFiles();
        $permanentFiles = array_filter($files, function ($file) {
            return is_null($file['maxAgeDays']);
        });

        self::assertCount(1, $permanentFiles);
        self::assertEquals(['tag1', 'tag2'], $permanentFiles[0]['tags']);
    }

    public function testListConfigsInBackup(): void
    {
        $restore = $this->createRestoreInstance('configurations');

        $componentId = 'keboola.csv-import';
        $configs = $restore->listConfigsInBackup($componentId);

        self::assertCount(1, $configs);
        self::assertEquals('213957449', reset($configs));

        // component not in backup
        $componentId = 'orchestrator';
        $configs = $restore->listConfigsInBackup($componentId);

        self::assertTrue(is_array($configs));
        self::assertCount(0, $configs);
    }

    public function testRestoreConfigurations(): void
    {
        $restore = $this->createRestoreInstance('configurations');
        $restore->restoreConfigs();

        $components = new Components($this->sapiClient);
        $componentsList = $components->listComponents();

        self::assertCount(2, $componentsList);
        self::assertEquals('keboola.csv-import', $componentsList[0]['id']);
        self::assertEquals('keboola.ex-slack', $componentsList[1]['id']);

        $config = $components->getConfiguration('keboola.csv-import', '213957449');

        /** @var array $expectedConfigData */
        $expectedConfigData = json_decode(
            (string) file_get_contents(
                __DIR__ . '/../prepareData/data/configurations/configurations/keboola.csv-import/213957449.json',
            ),
            true,
        );
        self::assertEquals(1, $config['version']);
        self::assertEquals('Configuration created', $config['changeDescription']);
        self::assertEquals('Accounts', $config['name']);
        self::assertEquals('Default CSV Importer', $config['description']);
        self::assertEquals(['key' => 'value'], $config['state']);
        self::assertEquals(
            $expectedConfigData['configuration'],
            $config['configuration'],
        );

        $config = $components->getConfiguration('keboola.ex-slack', '213957518');
        /** @var array $expectedConfigData */
        $expectedConfigData = json_decode(
            (string) file_get_contents(
                __DIR__ . '/../prepareData/data/configurations/configurations/keboola.ex-slack/213957518.json',
            ),
            true,
        );
        $expectedConfigData['configuration']['authorization']['oauth_api'] = [];
        self::assertEquals(2, $config['version']);
        self::assertEquals('Configuration 213957518 restored from backup', $config['changeDescription']);
        self::assertEmpty($config['state']);
        self::assertEquals($expectedConfigData['configuration'], $config['configuration']);
    }

    public function testRestoreConfigurationsWithoutVersions(): void
    {
        $restore = $this->createRestoreInstance('configurations-no-versions');
        $restore->restoreConfigs();

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
        $restore = $this->createRestoreInstance('configuration-skip');
        $restore->restoreConfigs(
            [
                'gooddata-writer',
                'orchestrator',
                'pigeon-importer',
            ],
        );

        $components = new Components($this->sapiClient);
        $componentsList = $components->listComponents();

        self::assertCount(1, $componentsList);
        self::assertEquals('keboola.csv-import', $componentsList[0]['id']);
        self::assertCount(1, $componentsList[0]['configurations']);
    }

    public function testRestoreEmptyObjectInConfiguration(): void
    {
        $restore = $this->createRestoreInstance('configuration-empty-object');
        $restore->restoreConfigs();

        $temp = new Temp();

        // empty array and object in config
        $file = $temp->createFile('config.json');
        $this->sapiClient->apiGet('components/keboola.csv-import/configs/1', $file->getPathname());
        /** @var stdClass $config */
        $config = json_decode((string) file_get_contents($file->getPathname()));
        self::assertEquals(new stdClass(), $config->configuration->emptyObject);
        self::assertEquals([], $config->configuration->emptyArray);
    }

    public function testRestoreConfigurationRows(): void
    {
        $restore = $this->createRestoreInstance('configuration-rows');
        $restore->restoreConfigs();

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
        $restore = $this->createRestoreInstance('configuration-rows');
        $restore->restoreConfigs();

        $temp = new Temp();

        // empty array and object in config rows
        $file = $temp->createFile('config.json');
        $this->sapiClient->apiGet('components/transformation/configs/1/rows', $file->getPathname());
        /** @var array $config */
        $config = json_decode((string) file_get_contents($file->getPathname()));
        self::assertEquals(new stdClass(), $config[0]->configuration->input[0]->datatypes);
        self::assertEquals([], $config[0]->configuration->queries);
    }

    public function testRestoreTableMetadata(): void
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

    public function testRestoreTransformationMetadata(): void
    {
        $restore = $this->createRestoreInstance('transformation-with-metadata');
        $restore->restoreConfigs();

        $components = new Components($this->branchAwareClient);

        $options = new ListConfigurationMetadataOptions();
        $options->setComponentId('keboola.snowflake-transformation');
        $options->setConfigurationId('sapi-php-test');

        $metadata = $components->listConfigurationMetadata($options);

        Assert::assertEquals('KBC.configuration.folderName', $metadata[0]['key']);
        Assert::assertEquals('testFolder', $metadata[0]['value']);
    }

    public function testRestoreTriggers(): void
    {
        $logger = new TestLogger();
        $restore = $this->createRestoreInstance('triggers', $logger);

        $restore->restoreBuckets();
        $restore->restoreTables();
        $restore->restoreTriggers();

        $triggers = $this->sapiClient->listTriggers();
        self::assertCount(1, $triggers);

        $expectedResult = <<<JSON
[
    {
        "id": "%s",
        "runWithTokenId": %d,
        "component": "keboola.orchestrator",
        "configurationId": "%s",
        "lastRun": "%s",
        "creatorToken": {
            "id": %d,
            "description": "%s"
        },
        "coolDownPeriodMinutes": 5,
        "tables": [
            {
                "tableId": "in.c-bucket.firstTable"
            }
        ]
    }
]
JSON;

        self::assertTrue($logger->hasInfo('Skipping trigger "1111" - no tables'));
        self::assertTrue($logger->hasInfo('Restoring trigger "2222"'));
        self::assertStringMatchesFormat($expectedResult, (string) json_encode($triggers, JSON_PRETTY_PRINT));
    }

    public function testRestoreNotifications(): void
    {
        $restore = $this->createRestoreInstance('notifications');
        $restore->restoreNotifications();

        $notificationClient = new NotificationClient(
            $this->sapiClient->getServiceUrl('notification'),
            $this->sapiClient->getTokenString(),
            [
                'backoffMaxTries' => 3,
                'userAgent' => 'Keboola Project Restore',
            ],
        );

        /** @var array[][] $expectedNotifications */
        $expectedNotifications = (array) json_decode(
            (string) file_get_contents(__DIR__ . '/../prepareData/data/notifications/notifications.json'),
            true,
        );

        $expectedNotifications[0]['id'] = '%s';
        $expectedNotifications[0]['filters'][0]['value'] = '%s';
        $expectedNotifications[1]['id'] = '%s';
        $expectedNotifications[1]['filters'][0]['value'] = '%s';

        $restoreNotifications = $notificationClient->listSubscriptions();

        self::assertEquals(2, count($restoreNotifications));
        self::assertStringMatchesFormat(
            (string) json_encode($expectedNotifications, JSON_PRETTY_PRINT),
            (string) json_encode($restoreNotifications, JSON_PRETTY_PRINT),
        );
    }

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
