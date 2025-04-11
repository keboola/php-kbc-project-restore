<?php

declare(strict_types=1);

namespace Keboola\ProjectRestore\Tests;

use Google\Cloud\Storage\StorageClient;
use Keboola\ProjectRestore\GcsRestore;
use Keboola\ProjectRestore\StorageApi\BucketInfo;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\DevBranchesMetadata;
use Keboola\StorageApi\Exception;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\Components\ListConfigurationMetadataOptions;
use Keboola\StorageApi\TableExporter;
use Keboola\Temp\Temp;
use PHPUnit\Framework\Assert;
use Psr\Log\Test\TestLogger;
use stdClass;

class CrossBackendTypedTableRestoreTest extends BaseTest
{
    private StorageClient $storageClient;

    public function setUp(): void
    {
        parent::setUp();

        $this->storageClient = new StorageClient([
            'keyFile' => json_decode((string)getenv('TEST_GCP_SERVICE_ACCOUNT'), true),
        ]);
    }


}
