<?php

declare(strict_types=1);

namespace Keboola\ProjectRestore\StorageApi;

use InvalidArgumentException;

class BucketInfo
{
    private string $id;

    private string $name;

    private string $stage;

    private string $backend;

    private string $description;

    private ?string $displayName;

    private bool $isLinkedBucket;

    private array $metadata = [];

    public function __construct(array $bucketInfo)
    {
        $this->id = $bucketInfo['id'];
        $this->name = $bucketInfo['name'];
        $this->stage = $bucketInfo['stage'];

        $this->backend = $bucketInfo['backend'];

        $this->displayName = $bucketInfo['displayName'] ?? null;

        $this->description = $bucketInfo['description'];
        $this->isLinkedBucket = isset($bucketInfo['sourceBucket']);

        if (!array_key_exists('metadata', $bucketInfo)) {
            throw new InvalidArgumentException(sprintf('Missing metadata info for bucket "%s"', $this->getId()));
        }

        $this->metadata = $bucketInfo['metadata'];
    }

    public function getId(): string
    {
        return $this->id;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function getStage(): string
    {
        return $this->stage;
    }

    public function getBackend(): string
    {
        return $this->backend;
    }

    public function getDescription(): ?string
    {
        return $this->description;
    }

    public function isLinkedBucket(): bool
    {
        return $this->isLinkedBucket;
    }

    public function getMetadata(): array
    {
        return $this->metadata;
    }

    public function getDisplayName(): ?string
    {
        return $this->displayName;
    }
}
