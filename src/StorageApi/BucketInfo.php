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
    private $stage;

    /**
     * @var string
     */
    private $backend;

    /**
     * @var string
     */
    private $description;

    public function __construct(array $bucketInfo)
    {
        $this->id = $bucketInfo['id'];
        $this->stage = $bucketInfo['stage'];

        $this->backend = $bucketInfo['backend'];

        $this->description = $bucketInfo['description'];
    }

    public function getId(): string
    {
        return $this->id;
    }

    public function getStage(): string
    {
        return $this->stage;
    }

    public function getBackend(): string
    {
        return $this->backend;
    }

    public function getDescription(): string
    {
        return $this->description;
    }
}