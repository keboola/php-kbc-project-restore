<?php

declare(strict_types=1);

namespace Keboola\ProjectRestore\Tests\StorageApi;

use Keboola\ProjectRestore\StorageApi\Token;
use Keboola\ProjectRestore\Tests\BaseTest;

class TokenTest extends BaseTest
{
    public function testToken(): void
    {
        $token = new Token($this->sapiClient);

        $tokenData = $this->sapiClient->verifyToken();

        $projectData = $tokenData['owner'];

        $this->assertEquals(getenv('TEST_STORAGE_API_TOKEN'), (string) $token);
        $this->assertEquals($tokenData['description'], $token->getDescription());

        $this->assertEquals($projectData['id'], $token->getProjectId());
        $this->assertEquals($projectData['name'], $token->getProjectName());

        $enabledBackends = [];
        $disabledBackends = [];

        if ($token->hasProjectMysqlBackend()) {
            $enabledBackends[] = 'mysql';
        } else {
            $disabledBackends[] = 'mysql';
        }

        if ($token->hasProjectRedshiftBackend()) {
            $enabledBackends[] = 'redshift';
        } else {
            $disabledBackends[] = 'redshift';
        }

        if ($token->hasProjectSnowflakeBackend()) {
            $enabledBackends[] = 'snowflake';
        } else {
            $disabledBackends[] = 'snowflake';
        }

        $this->assertGreaterThan(0, count($enabledBackends));
        $this->assertGreaterThan(0, count($disabledBackends));

        $this->assertTrue(in_array($projectData['defaultBackend'], $enabledBackends));
    }
}
