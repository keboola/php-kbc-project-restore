<?php

declare(strict_types=1);

namespace Keboola\ProjectRestore\StorageApi;

class BucketInfo
{
    /**
     * @var string
     */
    private $id;

    /**
     * @var string
     */
    private $name;

    /**
     * @var string
     */
    private $stage;

    /**
     * @var string
     */
    private $backend;

    /**
     * @var string
     */
    private $description;

    /**
     * @var array
     */
    private $attributes = [];

    /**
     * @var array
     */
    private $metadata = [];

    public function __construct(array $bucketInfo)
    {
        $this->id = $bucketInfo['id'];
        $this->name = $bucketInfo['name'];
        $this->stage = $bucketInfo['stage'];

        $this->backend = $bucketInfo['backend'];

        $this->description = $bucketInfo['description'];

        if (!array_key_exists('attributes', $bucketInfo)) {
            throw new \InvalidArgumentException(sprintf('Missing attributes info for bucket "%s"', $this->getId()));
        }

        if (!array_key_exists('metadata', $bucketInfo)) {
            throw new \InvalidArgumentException(sprintf('Missing metadata info for bucket "%s"', $this->getId()));
        }

        $this->attributes = $bucketInfo['attributes'];
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

    public function getAttributes(): array
    {
        return $this->attributes;
    }

    public function getMetadata(): array
    {
        return $this->metadata;
    }
}