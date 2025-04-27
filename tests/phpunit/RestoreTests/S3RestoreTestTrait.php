<?php

declare(strict_types=1);

namespace Keboola\ProjectRestore\Tests\RestoreTests;

use Aws\S3\S3Client;
use Keboola\ProjectRestore\S3Restore;
use Psr\Log\LoggerInterface;

trait S3RestoreTestTrait
{
    private S3Client $s3Client;

    public function setUp(): void
    {
        parent::setUp();

        $this->s3Client = new S3Client([
            'version' => '2006-03-01',
            'region' => (string) getenv('TEST_AWS_REGION'),
            'credentials' => [
                'key' => (string) getenv('TEST_AWS_ACCESS_KEY_ID'),
                'secret' => (string) getenv('TEST_AWS_SECRET_ACCESS_KEY'),
            ],
        ]);
    }

    protected function createRestoreInstance(string $postfix, ?LoggerInterface $logger = null): S3Restore
    {
        return new S3Restore(
            $this->sapiClient,
            $this->s3Client,
            (string) getenv('TEST_AWS_S3_BUCKET'),
            $postfix,
            $logger,
        );
    }
}
