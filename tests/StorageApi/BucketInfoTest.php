<?php

declare(strict_types=1);

namespace Keboola\ProjectRestore\Tests\StorageApi;

use Keboola\ProjectRestore\StorageApi\BucketInfo;
use Keboola\ProjectRestore\Tests\BaseTest;
use Keboola\StorageApi\Client;

class BucketInfoTest extends BaseTest
{
    public function testInfo(): void
    {
        $name = 'info-test';
        $stage = Client::STAGE_IN;
        $description = 'Some description for info bucket tests';

        $bucketId = $this->sapiClient->createBucket($name, $stage, $description);

        $bucketInfo = $this->sapiClient->getBucket($bucketId);

        $bucket = new BucketInfo($bucketInfo);
        $this->assertEquals($bucketId, $bucket->getId());
        $this->assertEquals($stage, $bucket->getStage());
        $this->assertEquals($bucketInfo['backend'], $bucket->getBackend());
        $this->assertEquals($description, $bucket->getDescription());
    }
}
