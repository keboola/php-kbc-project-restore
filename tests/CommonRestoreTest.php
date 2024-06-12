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
use Psr\Log\NullLogger;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Finder\Finder;
use Throwable;

class CommonRestoreTest extends TestCase
{
    /** @dataProvider createTableDefinitionExceptionsProvider */
    public function testHandleNullablePrimaryKeysIssue(
        Throwable $clientException,
        string $expectedExceptionClass,
        string $expectedExceptionMessage
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
                    ]
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
            ->method('listBuckets')
            ->willReturn([
                [
                    'id' => 'in.c-bucket',
                ],
            ]);

        $storageClientMock
            ->method('createTableDefinition')
            ->willThrowException($clientException);

        /**
         * @var Client $storageClientMock
         * @var BlobRestProxy $absClientMock
         */
        $restore = new AbsRestore(
            $storageClientMock,
            $absClientMock,
            'test-container',
            new NullLogger()
        );

        $this->expectException($expectedExceptionClass);
        $this->expectExceptionMessage($expectedExceptionMessage);
        $restore->restoreTables();
    }

    public function createTableDefinitionExceptionsProvider(): iterable
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
            ->willReturnCallback(function (string $container, string $filePath): ?GetBlobResult {
                return $this->createBlobResultMockFromFile('dry-run', $filePath);
            });

        $absClientMock
            ->method('listBlobs')
            ->willReturnCallback(function (string $container, ListBlobsOptions $options): ?ListBlobsResult {
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
            ->method('listBuckets')
            ->willReturn([
                [
                    'id' => 'in.c-bucket',
                ],
            ]);

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
            $logger
        );

        $restore->setDryRunMode();

        $restore->restoreProjectMetadata();
        $restore->restoreBuckets(false);
        $restore->restoreConfigs();

        $records = $logsHandler->getRecords();
        $logMessages = array_map(fn($log) => $log['message'], $records);
        $dryRunLogs = array_values(
            array_filter($logMessages, fn($message) => strpos($message, '[dry-run]') === 0)
        );

        // phpcs:disable Generic.Files.LineLength.MaxExceeded
        self::assertSame(
            [
                '[dry-run] Restore project metadata',
                '[dry-run] Restore bucket "in/c-bucket"',
                '[dry-run] Restore metadata of bucket "in/c-bucket" (provider "system")',
                '[dry-run] Restore metadata of bucket "in/c-bucket" (provider "system")',
                '[dry-run] Restore configuration 213957449 (component "keboola.csv-import")',
                '[dry-run] Restore state of configuration 213957449 (component "keboola.csv-import")',
                '[dry-run] Restore configuration 213957518 (component "keboola.ex-slack")',
                '[dry-run] Restore state of configuration 213957518 (component "keboola.ex-slack")',
                '[dry-run] Restore configuration sapi-php-test (component "keboola.snowflake-transformation")',
                '[dry-run] Restore state of configuration sapi-php-test (component "keboola.snowflake-transformation")',
                '[dry-run] Restore row 804561957 of configuration sapi-php-test (component "keboola.snowflake-transformation")',
                '[dry-run] Restore state of configuration row 804561957 (configuration sapi-php-test, component "keboola.snowflake-transformation")',
                '[dry-run] Restore metadata of configuration sapi-php-test (component "keboola.snowflake-transformation")',
                '[dry-run] Restore configuration sapi-php-test-2 (component "keboola.snowflake-transformation")',
                '[dry-run] Restore state of configuration sapi-php-test-2 (component "keboola.snowflake-transformation")',
                '[dry-run] Restore row 804561957 of configuration sapi-php-test-2 (component "keboola.snowflake-transformation")',
                '[dry-run] Restore state of configuration row 804561957 (configuration sapi-php-test-2, component "keboola.snowflake-transformation")',
                '[dry-run] Restore row 804561958 of configuration sapi-php-test-2 (component "keboola.snowflake-transformation")',
                '[dry-run] Restore state of configuration row 804561958 (configuration sapi-php-test-2, component "keboola.snowflake-transformation")',
                '[dry-run] Restore rows sort order (configuration sapi-php-test-2, component "keboola.snowflake-transformation")',
            ],
            $dryRunLogs,
        );
        // phpcs:enable Generic.Files.LineLength.MaxExceeded
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
        return $this->createBlobResultMock($content);
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

        $list = $this->createMock(ListBlobsResult::class);
        $list->method('getBlobs')
            ->willReturn($blobs);

        return $list;
    }
}
