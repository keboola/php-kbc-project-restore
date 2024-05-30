<?php

declare(strict_types=1);

namespace Keboola\ProjectRestore\Tests;

use Keboola\ProjectRestore\AbsRestore;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Exception as StorageApiException;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Blob\Models\GetBlobResult;
use Psr\Log\NullLogger;
use PHPUnit\Framework\TestCase;
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
}
