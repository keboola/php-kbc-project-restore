<?php

declare(strict_types=1);

namespace Keboola\ProjectRestore\Tests;

use Google\Cloud\Storage\StorageClient;
use Keboola\ProjectRestore\GcsRestore;

class CrossBackendRestoreTest extends BaseTest
{
    public const TEST_ITERATOR_SLICES_COUNT = 120;

    private StorageClient $storageClient;

    public function setUp(): void
    {
        parent::setUp();

        $this->storageClient = new StorageClient([
            'keyFile' => json_decode((string) getenv('TEST_GCP_SERVICE_ACCOUNT'), true),
        ]);
    }

    public function testRestoreTypedTableCrossBackends(): void
    {
        $restore = new GcsRestore(
            $this->sapiClient,
            $this->getListOfSignedUrls('typed-table-cross-backend'),
        );

        $restore->restoreBuckets(false);
        $restore->restoreTables();

        self::assertTrue($this->sapiClient->tableExists('in.c-bucket.firstTable'));

        $table = $this->sapiClient->getTable('in.c-bucket.firstTable');
        self::assertTrue($table['isTyped']);
        self::assertEquals(['1'], $table['definition']['primaryKeysNames']);
    }

    private function getListOfSignedUrls(string $string): array
    {
        $signedUrls = $this->storageClient
            ->bucket((string) getenv('TEST_GCP_BUCKET'))
            ->object($string . '/signedUrls.json');

        return (array) json_decode($signedUrls->downloadAsString(), true);
    }
}
