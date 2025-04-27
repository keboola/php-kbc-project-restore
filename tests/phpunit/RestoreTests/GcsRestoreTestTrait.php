<?php

declare(strict_types=1);

namespace Keboola\ProjectRestore\Tests\RestoreTests;

use Google\Cloud\Storage\StorageClient;
use Keboola\ProjectRestore\GcsRestore;
use Psr\Log\LoggerInterface;

trait GcsRestoreTestTrait
{
    private StorageClient $storageClient;

    public function setUp(): void
    {
        parent::setUp();

        $this->storageClient = new StorageClient([
            'keyFile' => json_decode((string) getenv('TEST_GCP_SERVICE_ACCOUNT'), true),
        ]);
    }

    protected function createRestoreInstance(string $postfix, ?LoggerInterface $logger = null): GcsRestore
    {
        return new GcsRestore(
            $this->sapiClient,
            $this->getListOfSignedUrls($postfix),
            $logger,
        );
    }

    private function getListOfSignedUrls(string $string): array
    {
        $signedUrls = $this->storageClient
            ->bucket((string) getenv('TEST_GCP_BUCKET'))
            ->object($string . '/signedUrls.json');

        return (array) json_decode($signedUrls->downloadAsString(), true);
    }
}
