<?php

declare(strict_types=1);

namespace Keboola\ProjectRestore\Tests\StorageApi;

use Keboola\ProjectRestore\StorageApi\BucketInfo;
use Keboola\ProjectRestore\StorageApi\Token;
use Keboola\ProjectRestore\Tests\BaseTest;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Metadata;

class BucketInfoTest extends BaseTest
{
    private const BUCKET_NAME = 'info-test';

    private const BUCKET_STAGE = Client::STAGE_IN;

    private const BUCKET_DESCR = 'Some description for info bucket tests';

    private function createTestBucket(): string
    {
        $bucketId = $this->sapiClient->createBucket(self::BUCKET_NAME, self::BUCKET_STAGE, self::BUCKET_DESCR);
        $this->sapiClient->setBucketAttribute($bucketId, 'foo', 'bar');

        $metadata = new Metadata($this->sapiClient);
        $metadata->postBucketMetadata($bucketId, 'tester', [
            [
                'key' => 'metaFoo',
                'value' => 'metaBar',
            ],
        ]);

        return $bucketId;
    }

    public function testErrorFromDetail(): void
    {
        $bucketId = $this->createTestBucket();
        $bucket = $this->sapiClient->getBucket($bucketId);

        try {
            new BucketInfo($bucket);
            $this->fail('Creating BucketInfo should fail on missing metadata info');
        } catch (\InvalidArgumentException $e) {
            $this->assertContains('Missing metadata info for bucket', $e->getMessage());
        }
    }

    public function testErrorFromList(): void
    {
        $this->createTestBucket();

        // missing attributes
        $buckets = $this->sapiClient->listBuckets(['include' => 'linkedBuckets']);
        $this->assertCount(1, $buckets);

        try {
            new BucketInfo($buckets[0]);
            $this->fail('Creating BucketInfo should fail on missing attributes info');
        } catch (\InvalidArgumentException $e) {
            $this->assertContains('Missing attributes info for bucket', $e->getMessage());
        }

        // missing metadata
        $buckets = $this->sapiClient->listBuckets(['include' => 'attributes']);
        $this->assertCount(1, $buckets);

        try {
            new BucketInfo($buckets[0]);
            $this->fail('Creating BucketInfo should fail on missing metadata info');
        } catch (\InvalidArgumentException $e) {
            $this->assertContains('Missing metadata info for bucket', $e->getMessage());
        }
    }

    public function testInfo(): void
    {
        $bucketId = $this->createTestBucket();

        $buckets = $this->sapiClient->listBuckets(['include' => 'attributes,metadata']);
        $this->assertCount(1, $buckets);

        $bucket = new BucketInfo($buckets[0]);
        $this->assertEquals($bucketId, $bucket->getId());
        $this->assertEquals('c-' . self::BUCKET_NAME, $bucket->getName());
        $this->assertEquals(self::BUCKET_NAME, $bucket->getDisplayName());
        $this->assertEquals(self::BUCKET_STAGE, $bucket->getStage());

        $this->assertEquals($buckets[0]['backend'], $bucket->getBackend());
        $this->assertEquals(self::BUCKET_DESCR, $bucket->getDescription());

        // attributes
        $attributes = $bucket->getAttributes();
        $this->assertCount(1, $attributes);

        $this->assertArrayHasKey('name', $attributes[0]);
        $this->assertEquals('foo', $attributes[0]['name']);

        $this->assertArrayHasKey('value', $attributes[0]);
        $this->assertEquals('bar', $attributes[0]['value']);

        $this->assertArrayHasKey('protected', $attributes[0]);
        $this->assertFalse($attributes[0]['protected']);

        // metadata
        $metadata = $bucket->getMetadata();
        $this->assertCount(1, $metadata);

        $this->assertArrayHasKey('key', $metadata[0]);
        $this->assertEquals('metaFoo', $metadata[0]['key']);

        $this->assertArrayHasKey('value', $metadata[0]);
        $this->assertEquals('metaBar', $metadata[0]['value']);
    }

    public function testLinkedInfo(): void
    {
        $token = new Token($this->sapiClient);

        $bucketId = $this->createTestBucket();

        $this->sapiClient->shareBucket($bucketId, ['sharing' => 'organization']);
        $linkedBucketId = $this->sapiClient->linkBucket(
            self::BUCKET_NAME,
            Client::STAGE_OUT,
            $token->getProjectId(),
            $bucketId
        );

        $buckets = $this->sapiClient->listBuckets(['include' => 'attributes,metadata']);
        $this->assertCount(2, $buckets);

        foreach ($buckets as $bucketInfo) {
            $bucket = new BucketInfo($bucketInfo);

            if ($bucket->getId() === $bucketId) {
                $this->assertFalse($bucket->isLinkedBucket());
            }
            if ($bucket->getId() === $linkedBucketId) {
                $this->assertTrue($bucket->isLinkedBucket());
            }
        }
    }
}
