<?php

declare(strict_types=1);

namespace Keboola\ProjectRestore\Tests;

use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client as StorageApi;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\DevBranches;
use PHPUnit\Framework\TestCase;

abstract class BaseTest extends TestCase
{
    protected StorageApi $sapiClient;

    protected StorageApi $branchAwareClient;

    public function setUp(): void
    {
        parent::setUp();

        $this->sapiClient = new StorageApi([
            'url' => getenv('TEST_STORAGE_API_URL'),
            'token' => getenv('TEST_STORAGE_API_TOKEN'),
        ]);

        $devBranches = new DevBranches($this->sapiClient);
        $listBranches = $devBranches->listBranches();
        $defaultBranch = current(array_filter($listBranches, fn($v) => $v['isDefault'] === true));

        $this->branchAwareClient = new BranchAwareClient(
            $defaultBranch['id'],
            [
                'url' => getenv('TEST_STORAGE_API_URL'),
                'token' => getenv('TEST_STORAGE_API_TOKEN'),
            ]
        );

        $this->cleanupKbcProject();
    }

    private function cleanupKbcProject(): void
    {
        $components = new Components($this->sapiClient);
        foreach ($components->listComponents() as $component) {
            foreach ($component['configurations'] as $configuration) {
                $components->deleteConfiguration($component['id'], $configuration['id']);

                // delete configuration from trash
                $components->deleteConfiguration($component['id'], $configuration['id']);
            }
        }

        // drop linked buckets
        foreach ($this->sapiClient->listBuckets() as $bucket) {
            if (isset($bucket['sourceBucket'])) {
                $this->sapiClient->dropBucket(
                    $bucket['id'],
                    [
                        'force' => true,
                        'async' => true,
                    ],
                );
            }
        }

        foreach ($this->sapiClient->listBuckets() as $bucket) {
            $this->sapiClient->dropBucket(
                $bucket['id'],
                [
                    'force' => true,
                    'async' => true,
                ],
            );
        }
    }
}
