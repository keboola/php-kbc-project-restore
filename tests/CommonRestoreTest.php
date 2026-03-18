<?php

declare(strict_types=1);

namespace Keboola\ProjectRestore\Tests;

use Keboola\ProjectRestore\AbsRestore;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Exception as StorageApiException;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Blob\Models\Blob;
use MicrosoftAzure\Storage\Blob\Models\GetBlobResult;
use MicrosoftAzure\Storage\Blob\Models\ListBlobsOptions;
use MicrosoftAzure\Storage\Blob\Models\ListBlobsResult;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Process\Process;
use Throwable;

class CommonRestoreTest extends TestCase
{
    /**
     * @dataProvider createTableDefinitionExceptionsProvider
     * @param class-string<Throwable> $expectedExceptionClass
     */
    public function testHandleNullablePrimaryKeysIssue(
        Throwable $clientException,
        string $expectedExceptionClass,
        string $expectedExceptionMessage,
    ): void {
        $absClientMock = $this->createMock(BlobRestProxy::class);
        $absClientMock
            ->method('getBlob')
            ->willReturn($this->createBlobResultMock(<<<JSON
                [
                  {
                    "id": "in.c-bucket.Account",
                    "name": "Account",
                    "isTyped": true,
                    "primaryKey": [
                      "Id"
                    ],
                    "isAlias": false,
                    "bucket": {
                      "id": "in.c-bucket",
                      "backend": "snowflake"
                    },
                    "columns": [
                      "Id"
                    ],
                    "columnMetadata": {
                      "Id": [
                        {"provider": "storage", "key": "KBC.datatype.type", "value": "VARCHAR"},
                        {"provider": "storage", "key": "KBC.datatype.nullable", "value": "0"},
                        {"provider": "storage", "key": "KBC.datatype.basetype", "value": "STRING"}
                      ]
                    }
                  }
                ]
            JSON));

        $storageClientMock = $this->createMock(Client::class);

        $storageClientMock
            ->method('apiGet')
            ->with('dev-branches/')
            ->willReturn([
                [
                    'id' => 123,
                    'isDefault' => true,
                ],
            ]);

        $storageClientMock
            ->method('getApiUrl')
            ->willReturn('https://connection');

        $storageClientMock
            ->method('getTokenString')
            ->willReturn('token');

        $storageClientMock
            ->method('verifyToken')
            ->willReturn([
                'id' => '123',
                'description' => 'test',
                'owner' => ['id' => 1, 'name' => 'test'],
            ]);

        $storageClientMock->token = 'test-token';

        $storageClientMock
            ->method('listBuckets')
            ->willReturn([
                [
                    'id' => 'in.c-bucket',
                    'backend' => 'snowflake',
                ],
            ]);

        /**
         * @var Client $storageClientMock
         * @var BlobRestProxy $absClientMock
         */
        $restore = new AbsRestore(
            $storageClientMock,
            $absClientMock,
            'test-container',
            new NullLogger(),
        );

        $isNullablePkError = $clientException instanceof ClientException
            && $clientException->getCode() === 400
            && (bool) preg_match('/Primary keys columns must be set nullable false/', $clientException->getMessage());

        $restore->setWorkerProcessFactory(
            fn(array $input): Process => $this->createErrorProcessMock(
                $clientException->getMessage(),
                $clientException instanceof ClientException ? $clientException->getCode() : 0,
                $isNullablePkError,
                $clientException instanceof ClientException,
            ),
        );

        $this->expectException($expectedExceptionClass);
        $this->expectExceptionMessage($expectedExceptionMessage);
        $restore->restoreTables();
    }

    public static function createTableDefinitionExceptionsProvider(): iterable
    {
        yield 'nullable-primary-keys-issue' => [
            new ClientException(
                "Invalid request:\n - primaryKeysNames: \"Primary keys on columns [ID] cannot be created."
                . ' Primary keys columns must be set nullable false."',
                400,
            ),
            StorageApiException::class,
            'Table "Account" cannot be restored because the primary key cannot be set on a nullable column.',
        ];

        yield 'other-issue' => [
            new ClientException('Something went wrong', 400),
            ClientException::class,
            'Something went wrong',
        ];

        yield 'other-issue-2' => [
            new ClientException('Service Unavailable', 503),
            ClientException::class,
            'Service Unavailable',
        ];
    }

    public function testDryRunMode(): void
    {
        $logsHandler = new TestHandler();
        $logger = new Logger('tests', [$logsHandler]);

        $absClientMock = $this->createMock(BlobRestProxy::class);

        $absClientMock
            ->method('getBlob')
            ->willReturnCallback(function (string $container, string $filePath): GetBlobResult {
                return $this->createBlobResultMockFromFile('dry-run', $filePath);
            });

        $absClientMock
            ->method('listBlobs')
            ->willReturnCallback(function (string $container, ListBlobsOptions $options): ListBlobsResult {
                return $this->createListBlobsResultMockFromFile('dry-run', $options->getPrefix());
            });

        $storageClientMock = $this->createMock(Client::class);

        $storageClientMock
            ->method('apiGet')
            ->with('dev-branches/')
            ->willReturn([
                [
                    'id' => 123,
                    'isDefault' => true,
                ],
            ]);

        $storageClientMock
            ->method('getApiUrl')
            ->willReturn('https://connection');

        $storageClientMock
            ->method('getTokenString')
            ->willReturn('token');

        $storageClientMock
            ->method('verifyToken')
            ->willReturn([
                'id' => '123',
                'description' => 'test',
                'owner' => ['id' => 1, 'name' => 'test', 'defaultBackend' => 'snowflake'],
            ]);

        $storageClientMock->token = 'test-token';

        $storageClientMock
            ->method('listBuckets')
            ->willReturn([
                [
                    'id' => 'in.c-bucket',
                    'backend' => 'snowflake',
                ],
                [
                    'id' => 'out.c-bucket',
                    'backend' => 'snowflake',
                ],
            ]);

        $storageClientMock
            ->method('tableExists')
            ->willReturn(true);

        $storageClientMock
            ->method('indexAction')
            ->willReturn([
                'components' => [
                    [
                        'id' => 'keboola.csv-import',
                    ],
                    [
                        'id' => 'keboola.ex-slack',
                    ],
                    [
                        'id' => 'keboola.snowflake-transformation',
                    ],
                ],
            ]);

        /**
         * @var Client $storageClientMock
         * @var BlobRestProxy $absClientMock
         */
        $restore = new AbsRestore(
            $storageClientMock,
            $absClientMock,
            'test-container',
            $logger,
        );

        $restore->setDryRunMode();

        $restore->restoreProjectMetadata();
        $restore->restoreBuckets(false);
        $restore->restoreConfigs();
        $restore->restoreTables();
        $restore->restoreTableAliases();

        $records = $logsHandler->getRecords();
        /** @var string[] $logMessages */
        $logMessages = array_map(fn($log) => $log['message'], $records);
        $dryRunLogs = array_values(
            array_filter($logMessages, fn(string $message) => str_contains($message, '[dry-run]')),
        );

        // phpcs:disable Generic.Files.LineLength.MaxExceeded
        self::assertSame(
            [
                '[dry-run] Restore project metadata',
                '[dry-run] Restore bucket "in/c-bucket"',
                '[dry-run] Restore metadata of bucket "in/c-bucket" (provider "system")',
                '[dry-run] Restore metadata of bucket "in/c-bucket" (provider "system")',
                '[dry-run] Create configuration 213957449 (component "keboola.csv-import")',
                '[dry-run] Update configuration 213957449 (component "keboola.csv-import")',
                '[dry-run] Restore state of configuration 213957449 (component "keboola.csv-import")',
                '[dry-run] Create configuration 213957518 (component "keboola.ex-slack")',
                '[dry-run] Update configuration 213957518 (component "keboola.ex-slack")',
                '[dry-run] Restore state of configuration 213957518 (component "keboola.ex-slack")',
                '[dry-run] Create configuration sapi-php-test (component "keboola.snowflake-transformation")',
                '[dry-run] Update configuration sapi-php-test (component "keboola.snowflake-transformation")',
                '[dry-run] Restore state of configuration sapi-php-test (component "keboola.snowflake-transformation")',
                '[dry-run] Create configuration row 804561957 (configuration sapi-php-test, component "keboola.snowflake-transformation")',
                '[dry-run] Update row 804561957 of configuration sapi-php-test (component "keboola.snowflake-transformation")',
                '[dry-run] Restore state of configuration row 804561957 (configuration sapi-php-test, component "keboola.snowflake-transformation")',
                '[dry-run] Restore metadata of configuration sapi-php-test (component "keboola.snowflake-transformation")',
                '[dry-run] Create configuration sapi-php-test-2 (component "keboola.snowflake-transformation")',
                '[dry-run] Update configuration sapi-php-test-2 (component "keboola.snowflake-transformation")',
                '[dry-run] Restore state of configuration sapi-php-test-2 (component "keboola.snowflake-transformation")',
                '[dry-run] Create configuration row 804561957 (configuration sapi-php-test-2, component "keboola.snowflake-transformation")',
                '[dry-run] Update row 804561957 of configuration sapi-php-test-2 (component "keboola.snowflake-transformation")',
                '[dry-run] Restore state of configuration row 804561957 (configuration sapi-php-test-2, component "keboola.snowflake-transformation")',
                '[dry-run] Create configuration row 804561958 (configuration sapi-php-test-2, component "keboola.snowflake-transformation")',
                '[dry-run] Update row 804561958 of configuration sapi-php-test-2 (component "keboola.snowflake-transformation")',
                '[dry-run] Restore state of configuration row 804561958 (configuration sapi-php-test-2, component "keboola.snowflake-transformation")',
                '[dry-run] Restore rows sort order (configuration sapi-php-test-2, component "keboola.snowflake-transformation")',
                '[dry-run] Restore table in.c-bucket.Account',
                '[dry-run] Restore alias out.c-bucket.Account',
            ],
            $dryRunLogs,
        );
        // phpcs:enable Generic.Files.LineLength.MaxExceeded
    }

    public function testRestoreTypedTableSnowflakeToBigqueryThrowsForNumericScaleOver9(): void
    {
        $tableJson = json_encode([[
            'id' => 'in.c-bucket.amounts',
            'name' => 'amounts',
            'isTyped' => true,
            'isAlias' => false,
            'primaryKey' => [],
            'columns' => ['amount'],
            'bucket' => ['id' => 'in.c-bucket', 'backend' => 'snowflake'],
            'columnMetadata' => [
                'amount' => [
                    ['provider' => 'storage', 'key' => 'KBC.datatype.type', 'value' => 'NUMBER'],
                    ['provider' => 'storage', 'key' => 'KBC.datatype.nullable', 'value' => '0'],
                    ['provider' => 'storage', 'key' => 'KBC.datatype.length', 'value' => '38,12'],
                    ['provider' => 'storage', 'key' => 'KBC.datatype.basetype', 'value' => 'NUMERIC'],
                ],
            ],
        ]]);

        $absClientMock = $this->createMock(BlobRestProxy::class);
        $absClientMock->method('getBlob')->willReturn($this->createBlobResultMock((string) $tableJson));

        $storageClientMock = $this->createMock(Client::class);
        $storageClientMock->method('apiGet')->with('dev-branches/')->willReturn([[
            'id' => 123,
            'isDefault' => true,
        ]]);
        $storageClientMock->method('getApiUrl')->willReturn('https://connection');
        $storageClientMock->method('getTokenString')->willReturn('token');
        $storageClientMock->method('verifyToken')->willReturn([
            'id' => '123',
            'description' => 'test',
            'owner' => ['id' => 1, 'name' => 'test'],
        ]);
        $storageClientMock->token = 'test-token';
        $storageClientMock->method('listBuckets')->willReturn([
            ['id' => 'in.c-bucket', 'backend' => 'bigquery'],
        ]);
        /** @var Client $storageClientMock */
        /** @var BlobRestProxy $absClientMock */
        $restore = new AbsRestore($storageClientMock, $absClientMock, 'test-container', new NullLogger());

        $this->expectException(StorageApiException::class);
        $this->expectExceptionMessage('Column "amount" has type NUMBER(38,12)');
        $restore->restoreTables();
    }

    public function testRestoreTypedTableSnowflakeToBigqueryAllowsNumericScaleOf9(): void
    {
        $tableJson = json_encode([[
            'id' => 'in.c-bucket.amounts',
            'name' => 'amounts',
            'isTyped' => true,
            'isAlias' => false,
            'primaryKey' => [],
            'columns' => ['amount'],
            'bucket' => ['id' => 'in.c-bucket', 'backend' => 'snowflake'],
            'columnMetadata' => [
                'amount' => [
                    ['provider' => 'storage', 'key' => 'KBC.datatype.type', 'value' => 'NUMBER'],
                    ['provider' => 'storage', 'key' => 'KBC.datatype.nullable', 'value' => '0'],
                    ['provider' => 'storage', 'key' => 'KBC.datatype.length', 'value' => '38,9'],
                    ['provider' => 'storage', 'key' => 'KBC.datatype.basetype', 'value' => 'NUMERIC'],
                ],
            ],
        ]]);

        $absClientMock = $this->createMock(BlobRestProxy::class);
        $absClientMock->method('getBlob')->willReturn($this->createBlobResultMock((string) $tableJson));
        $listBlobsResult = $this->createMock(ListBlobsResult::class);
        $listBlobsResult->method('getBlobs')->willReturn([]);
        $absClientMock->method('listBlobs')->willReturn($listBlobsResult);

        $storageClientMock = $this->createMock(Client::class);
        $storageClientMock->method('apiGet')->with('dev-branches/')->willReturn([[
            'id' => 123,
            'isDefault' => true,
        ]]);
        $storageClientMock->method('getApiUrl')->willReturn('https://connection');
        $storageClientMock->method('getTokenString')->willReturn('token');
        $storageClientMock->method('verifyToken')->willReturn([
            'id' => '123',
            'description' => 'test',
            'owner' => ['id' => 1, 'name' => 'test'],
        ]);
        $storageClientMock->token = 'test-token';
        $storageClientMock->method('listBuckets')->willReturn([
            ['id' => 'in.c-bucket', 'backend' => 'bigquery'],
        ]);
        /** @var Client $storageClientMock */
        /** @var BlobRestProxy $absClientMock */
        $restore = new AbsRestore($storageClientMock, $absClientMock, 'test-container', new NullLogger());

        /** @var array<string, mixed>|null $capturedWorkerInput */
        $capturedWorkerInput = null;
        $restore->setWorkerProcessFactory(function (array $input) use (&$capturedWorkerInput): Process {
            $capturedWorkerInput = $input;
            return $this->createSuccessProcessMock('in.c-bucket.amounts');
        });

        $restore->restoreTables();

        self::assertNotNull($capturedWorkerInput);
        self::assertTrue($capturedWorkerInput['isTyped']);
        // table definition should have been built (cross-backend Snowflake→BigQuery)
        self::assertArrayHasKey('tableDefinition', $capturedWorkerInput);
    }

    public function testForcePrimaryKeyNotNullOverridesNullable(): void
    {
        $tableJson = json_encode([[
            'id' => 'in.c-bucket.Account',
            'name' => 'Account',
            'isTyped' => true,
            'isAlias' => false,
            'primaryKey' => ['Id'],
            'columns' => ['Id', 'Name'],
            'bucket' => ['id' => 'in.c-bucket', 'backend' => 'snowflake'],
            'columnMetadata' => [
                'Id' => [
                    ['provider' => 'storage', 'key' => 'KBC.datatype.type', 'value' => 'VARCHAR'],
                    ['provider' => 'storage', 'key' => 'KBC.datatype.nullable', 'value' => '1'],
                    ['provider' => 'storage', 'key' => 'KBC.datatype.basetype', 'value' => 'STRING'],
                ],
                'Name' => [
                    ['provider' => 'storage', 'key' => 'KBC.datatype.type', 'value' => 'VARCHAR'],
                    ['provider' => 'storage', 'key' => 'KBC.datatype.nullable', 'value' => '1'],
                    ['provider' => 'storage', 'key' => 'KBC.datatype.basetype', 'value' => 'STRING'],
                ],
            ],
        ]]);

        $logsHandler = new TestHandler();
        $logger = new Logger('tests', [$logsHandler]);

        $absClientMock = $this->createMock(BlobRestProxy::class);
        $absClientMock->method('getBlob')->willReturn($this->createBlobResultMock((string) $tableJson));
        $listBlobsResult = $this->createMock(ListBlobsResult::class);
        $listBlobsResult->method('getBlobs')->willReturn([]);
        $absClientMock->method('listBlobs')->willReturn($listBlobsResult);

        $storageClientMock = $this->createMock(Client::class);
        $storageClientMock->method('apiGet')->with('dev-branches/')->willReturn([[
            'id' => 123,
            'isDefault' => true,
        ]]);
        $storageClientMock->method('getApiUrl')->willReturn('https://connection');
        $storageClientMock->method('getTokenString')->willReturn('token');
        $storageClientMock->method('verifyToken')->willReturn([
            'id' => '123',
            'description' => 'test',
            'owner' => ['id' => 1, 'name' => 'test'],
        ]);
        $storageClientMock->token = 'test-token';
        $storageClientMock->method('listBuckets')->willReturn([
            ['id' => 'in.c-bucket', 'backend' => 'snowflake'],
        ]);

        /** @var Client $storageClientMock */
        /** @var BlobRestProxy $absClientMock */
        $restore = new AbsRestore($storageClientMock, $absClientMock, 'test-container', $logger);
        $restore->setForcePrimaryKeyNotNull(true);

        /** @var array<string, mixed>|null $capturedWorkerInput */
        $capturedWorkerInput = null;
        $restore->setWorkerProcessFactory(function (array $input) use (&$capturedWorkerInput): Process {
            $capturedWorkerInput = $input;
            return $this->createSuccessProcessMock('in.c-bucket.Account');
        });

        $restore->restoreTables();

        self::assertNotNull($capturedWorkerInput);
        /** @var array{columns: array<int, array{name: string, definition: array{nullable: bool}}>} $tableDefinition */
        $tableDefinition = $capturedWorkerInput['tableDefinition'];
        $idColumnMatches = array_values(
            array_filter($tableDefinition['columns'], fn(array $c): bool => $c['name'] === 'Id'),
        );
        $nameColumnMatches = array_values(
            array_filter($tableDefinition['columns'], fn(array $c): bool => $c['name'] === 'Name'),
        );
        self::assertCount(1, $idColumnMatches);
        self::assertCount(1, $nameColumnMatches);
        // PK column must be forced to NOT NULL
        self::assertFalse($idColumnMatches[0]['definition']['nullable']);
        // non-PK column must remain nullable
        self::assertTrue($nameColumnMatches[0]['definition']['nullable']);

        $messages = array_map(fn($r) => $r['message'], $logsHandler->getRecords());
        self::assertContains(
            'Table "Account": primary key column "Id" is nullable in source, forcing NOT NULL in destination.',
            $messages,
        );
    }

    public function testCheckTableRestorableLogsInfoWhenForcePrimaryKeyNotNull(): void
    {
        $tableJson = json_encode([[
            'id' => 'in.c-bucket.Account',
            'name' => 'Account',
            'isTyped' => true,
            'isAlias' => false,
            'primaryKey' => ['Id'],
            'columns' => ['Id'],
            'bucket' => ['id' => 'in.c-bucket', 'backend' => 'snowflake'],
            'columnMetadata' => [
                'Id' => [
                    ['provider' => 'storage', 'key' => 'KBC.datatype.type', 'value' => 'VARCHAR'],
                    ['provider' => 'storage', 'key' => 'KBC.datatype.nullable', 'value' => '1'],
                    ['provider' => 'storage', 'key' => 'KBC.datatype.basetype', 'value' => 'STRING'],
                ],
            ],
        ]]);

        $logsHandler = new TestHandler();
        $logger = new Logger('tests', [$logsHandler]);

        $absClientMock = $this->createMock(BlobRestProxy::class);
        $absClientMock->method('getBlob')->willReturn($this->createBlobResultMock((string) $tableJson));
        $listBlobsResult = $this->createMock(ListBlobsResult::class);
        $listBlobsResult->method('getBlobs')->willReturn([]);
        $absClientMock->method('listBlobs')->willReturn($listBlobsResult);

        $storageClientMock = $this->createMock(Client::class);
        $storageClientMock->method('apiGet')->with('dev-branches/')->willReturn([[
            'id' => 123,
            'isDefault' => true,
        ]]);
        $storageClientMock->method('getApiUrl')->willReturn('https://connection');
        $storageClientMock->method('getTokenString')->willReturn('token');
        $storageClientMock->method('verifyToken')->willReturn([
            'id' => '123',
            'description' => 'test',
            'owner' => ['id' => 1, 'name' => 'test'],
        ]);
        $storageClientMock->token = 'test-token';
        $storageClientMock->method('listBuckets')->willReturn([
            ['id' => 'in.c-bucket', 'backend' => 'snowflake'],
        ]);
        /** @var Client $storageClientMock */
        /** @var BlobRestProxy $absClientMock */
        $restore = new AbsRestore($storageClientMock, $absClientMock, 'test-container', $logger);
        $restore->setForcePrimaryKeyNotNull(true);
        $restore->setWorkerProcessFactory(
            fn(array $input): Process => $this->createSuccessProcessMock('in.c-bucket.Account'),
        );
        $restore->restoreTables();

        $infoMessages = array_map(
            fn($r) => $r['message'],
            array_filter($logsHandler->getRecords(), fn($r) => $r['level'] === 200), // INFO = 200
        );
        self::assertContains(
            'Table "Account": primary key column "Id" is nullable, will be forced to NOT NULL.',
            $infoMessages,
        );

        // must NOT log the warning
        $warnMessages = array_map(
            fn($r) => $r['message'],
            array_filter($logsHandler->getRecords(), fn($r) => $r['level'] === 300), // WARNING = 300
        );
        self::assertNotContains(
            'Table "Account" cannot be restored because the primary key column "Id" is nullable.',
            $warnMessages,
        );
    }

    public function testCheckTableRestorableLogsWarningWithoutForcePrimaryKeyNotNull(): void
    {
        $tableJson = json_encode([[
            'id' => 'in.c-bucket.Account',
            'name' => 'Account',
            'isTyped' => true,
            'isAlias' => false,
            'primaryKey' => ['Id'],
            'columns' => ['Id'],
            'bucket' => ['id' => 'in.c-bucket', 'backend' => 'snowflake'],
            'columnMetadata' => [
                'Id' => [
                    ['provider' => 'storage', 'key' => 'KBC.datatype.type', 'value' => 'VARCHAR'],
                    ['provider' => 'storage', 'key' => 'KBC.datatype.nullable', 'value' => '1'],
                    ['provider' => 'storage', 'key' => 'KBC.datatype.basetype', 'value' => 'STRING'],
                ],
            ],
        ]]);

        $logsHandler = new TestHandler();
        $logger = new Logger('tests', [$logsHandler]);

        $absClientMock = $this->createMock(BlobRestProxy::class);
        $absClientMock->method('getBlob')->willReturn($this->createBlobResultMock((string) $tableJson));
        $listBlobsResult = $this->createMock(ListBlobsResult::class);
        $listBlobsResult->method('getBlobs')->willReturn([]);
        $absClientMock->method('listBlobs')->willReturn($listBlobsResult);

        $storageClientMock = $this->createMock(Client::class);
        $storageClientMock->method('apiGet')->with('dev-branches/')->willReturn([[
            'id' => 123,
            'isDefault' => true,
        ]]);
        $storageClientMock->method('getApiUrl')->willReturn('https://connection');
        $storageClientMock->method('getTokenString')->willReturn('token');
        $storageClientMock->method('verifyToken')->willReturn([
            'id' => '123',
            'description' => 'test',
            'owner' => ['id' => 1, 'name' => 'test'],
        ]);
        $storageClientMock->token = 'test-token';
        $storageClientMock->method('listBuckets')->willReturn([
            ['id' => 'in.c-bucket', 'backend' => 'snowflake'],
        ]);
        /** @var Client $storageClientMock */
        /** @var BlobRestProxy $absClientMock */
        $restore = new AbsRestore($storageClientMock, $absClientMock, 'test-container', $logger);
        // setForcePrimaryKeyNotNull NOT called — default behaviour
        $restore->setWorkerProcessFactory(
            fn(array $input): Process => $this->createSuccessProcessMock('in.c-bucket.Account'),
        );
        $restore->restoreTables();

        $warnMessages = array_map(
            fn($r) => $r['message'],
            array_filter($logsHandler->getRecords(), fn($r) => $r['level'] === 300), // WARNING = 300
        );
        self::assertContains(
            'Table "Account" cannot be restored because the primary key column "Id" is nullable.',
            $warnMessages,
        );
    }

    private function createSuccessProcessMock(string $tableId): Process
    {
        $process = $this->createMock(Process::class);
        $process->method('start')->willReturn(null);
        $process->method('isRunning')->willReturn(false);
        $process->method('getExitCode')->willReturn(0);
        $process->method('getOutput')->willReturn((string) json_encode([
            'tableId' => $tableId,
            'error' => null,
        ]));
        /** @var Process $process */
        return $process;
    }

    private function createErrorProcessMock(
        string $errorMessage,
        int $code,
        bool $isNullablePkError,
        bool $isClientException,
    ): Process {
        $process = $this->createMock(Process::class);
        $process->method('start')->willReturn(null);
        $process->method('isRunning')->willReturn(false);
        $process->method('getExitCode')->willReturn(1);
        $process->method('getOutput')->willReturn((string) json_encode([
            'tableId' => null,
            'error' => $errorMessage,
            'code' => $code,
            'isNullablePkError' => $isNullablePkError,
            'isClientException' => $isClientException,
        ]));
        /** @var Process $process */
        return $process;
    }

    private function createBlobResultMock(string $content): GetBlobResult
    {
        /** @var resource $resource */
        $resource = fopen('php://memory', 'rb+');
        fwrite($resource, $content);
        rewind($resource);

        $blobResultMock = $this->createMock(GetBlobResult::class);
        $blobResultMock
            ->method('getContentStream')
            ->willReturn($resource);

        /** @var GetBlobResult $blobResultMock */
        return $blobResultMock;
    }

    private function createBlobResultMockFromFile(string $backupName, string $filePath): GetBlobResult
    {
        $content = file_get_contents(sprintf('%s/data/backups/%s/%s', __DIR__, $backupName, $filePath));
        return $this->createBlobResultMock($content ?: '');
    }

    private function createListBlobsResultMockFromFile(string $backupName, string $pathPrefix): ListBlobsResult
    {
        $dir = sprintf('%s/data/backups/%s/%s', __DIR__, $backupName, $pathPrefix);
        $finder = new Finder();
        $blobs = [];
        foreach ($finder->files()->in($dir) as $file) {
            $name = sprintf('%s/%s', $pathPrefix, $file->getFilename());
            $blob = $this->createMock(Blob::class);
            $blob->method('getName')->willReturn($name);
            $blobs[] = $blob;
        }

        $listMock = $this->createMock(ListBlobsResult::class);
        $listMock->method('getBlobs')
            ->willReturn($blobs);

        /** @var ListBlobsResult $listMock */
        return $listMock;
    }
}
