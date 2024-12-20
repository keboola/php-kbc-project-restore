<?php

declare(strict_types=1);

namespace Keboola\ProjectRestore\StorageApi;

use Keboola\StorageApi\Client;

class Token
{
    private array $tokenData;

    private string $token;

    public function __construct(Client $sapiClient)
    {
        $this->tokenData = $sapiClient->verifyToken();
        $this->token = $sapiClient->token;
    }

    /**
     * Convert to SAPI Token
     */
    public function __toString(): string
    {
        return $this->token;
    }

    public function getId(): string
    {
        return $this->tokenData['id'];
    }

    public function getDescription(): string
    {
        return $this->tokenData['description'];
    }

    public function getProjectId(): int
    {
        return $this->tokenData['owner']['id'];
    }

    public function getProjectName(): string
    {
        return $this->tokenData['owner']['name'];
    }

    public function hasProjectMysqlBackend(): bool
    {
        return isset($this->tokenData['owner']['hasMysql']) && $this->tokenData['owner']['hasMysql'] === true;
    }

    public function hasProjectRedshiftBackend(): bool
    {
        return isset($this->tokenData['owner']['hasRedshift']) && $this->tokenData['owner']['hasRedshift'] === true;
    }

    public function hasProjectSnowflakeBackend(): bool
    {
        return isset($this->tokenData['owner']['hasSnowflake']) && $this->tokenData['owner']['hasSnowflake'] === true;
    }
}
