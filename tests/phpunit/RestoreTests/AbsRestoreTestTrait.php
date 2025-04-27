<?php

declare(strict_types=1);

namespace Keboola\ProjectRestore\Tests\RestoreTests;

use Keboola\ProjectRestore\AbsRestore;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use Psr\Log\LoggerInterface;

trait AbsRestoreTestTrait
{
    private BlobRestProxy $blobClient;

    public function setUp(): void
    {
        parent::setUp();

        $this->blobClient = BlobRestProxy::createBlobService(
            sprintf(
                'DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=core.windows.net',
                (string) getenv('TEST_AZURE_ACCOUNT_NAME'),
                (string) getenv('TEST_AZURE_ACCOUNT_KEY'),
            ),
        );
    }

    protected function createRestoreInstance(string $postfix, ?LoggerInterface $logger = null): AbsRestore
    {
        return new AbsRestore(
            $this->sapiClient,
            $this->blobClient,
            (string) getenv('TEST_AZURE_CONTAINER_NAME') . '-' . $postfix,
            $logger,
        );
    }
}
