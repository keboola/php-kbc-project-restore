<?php

declare(strict_types=1);

namespace Keboola\ProjectRestore\Tests\StorageApi;

use Keboola\ProjectRestore\StorageApi\ConfigurationCorrector;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class ConfigurationCorrectorTest extends TestCase
{
    /** @dataProvider componentConfigurationsData */
    public function testConfigurationCorrector(
        string $componentId,
        string $apiUrl,
        array $inConfigData,
        array $expectedOutConfigData,
        string $backendType
    ): void {
        $corrector = new ConfigurationCorrector($apiUrl, new NullLogger());
        $correctedConfigData = $corrector->correct(
            $componentId,
            json_decode((string) json_encode($inConfigData)),
            $backendType
        );
        $this->assertEquals(json_decode((string) json_encode($expectedOutConfigData)), $correctedConfigData);
    }

    public function componentConfigurationsData(): iterable
    {
        yield 'with-oauth' => [
            'some-component',
            'https://connection.keboola.com',
            [
                'storage' => [
                    'input' => [],
                ],
                'parameters' => [
                    'something' => 'value',
                ],
                'authorization' => [
                    'oauth_api' => [
                        'id' => '123',
                    ],
                ],
            ],
            [
                'storage' => [
                    'input' => [],
                ],
                'parameters' => [
                    'something' => 'value',
                ],
                'authorization' => [
                    'oauth_api' => (object) [],
                ],
            ],
            'snowflake',
        ];

        yield 'without-oauth' => [
            'some-component',
            'https://connection.keboola.com',
            [
                'storage' => [
                    'input' => [],
                ],
                'parameters' => [
                    'something' => 'value',
                ],
            ],
            [
                'storage' => [
                    'input' => [],
                ],
                'parameters' => [
                    'something' => 'value',
                ],
            ],
            'snowflake',
        ];

        yield 'with-weird-oauth' => [
            'some-component',
            'https://connection.keboola.com',
            [
                'storage' => [
                    'input' => [],
                ],
                'parameters' => [
                    'something' => 'value',
                ],
                'authorization' => [
                    'oauth_api_v123' => [
                        'something' => 'another',
                    ],
                ],
            ],
            [
                'storage' => [
                    'input' => [],
                ],
                'parameters' => [
                    'something' => 'value',
                ],
                'authorization' => [
                    'oauth_api_v123' => [
                        'something' => 'another',
                    ],
                ],
            ],
            'snowflake',
        ];

        yield 'with-injection-oauth' => [
            'some-component',
            'https://connection.keboola.com',
            [
                'storage' => [
                    'input' => [],
                ],
                'parameters' => [
                    'something' => 'value',
                ],
                'authorization' => [
                    'oauth_api' => [
                        'credentials' => [
                            '#data' => 'value',
                        ],
                    ],
                ],
            ],
            [
                'storage' => [
                    'input' => [],
                ],
                'parameters' => [
                    'something' => 'value',
                ],
                'authorization' => [
                    'oauth_api' => [
                        'credentials' => [
                            '#data' => 'value',
                        ],
                    ],
                ],
            ],
            'snowflake',
        ];

        yield 'orchestrator-with-stack-specific-component-snflk-backend' => [
            'keboola.orchestrator',
            'https://connection.us-east4.gcp.keboola.com',
            [
                'phases' => [],
                'tasks' => [
                    [
                        'id' => 123,
                        'task' => [
                            'componentId' => 'keboola.wr-db-snowflake',
                            'configId' => '123',
                        ],
                    ],
                ],
            ],
            [
                'phases' => [],
                'tasks' => [
                    [
                        'id' => 123,
                        'task' => [
                            'componentId' => 'keboola.wr-db-snowflake-gcs-s3',
                            'configId' => '123',
                        ],
                    ],
                ],
            ],
            'snowflake',
        ];

        yield 'orchestrator-with-stack-specific-component-bq-backend' => [
            'keboola.orchestrator',
            'https://connection.us-east4.gcp.keboola.com',
            [
                'phases' => [],
                'tasks' => [
                    [
                        'id' => 123,
                        'task' => [
                            'componentId' => 'keboola.wr-db-snowflake',
                            'configId' => '123',
                        ],
                    ],
                ],
            ],
            [
                'phases' => [],
                'tasks' => [
                    [
                        'id' => 123,
                        'task' => [
                            'componentId' => 'keboola.wr-db-snowflake-gcs',
                            'configId' => '123',
                        ],
                    ],
                ],
            ],
            'bigquery',
        ];

        yield 'orchestrator-with-stack-specific-component-2' => [
            'keboola.orchestrator',
            'https://connection.us-central1.gcp.keboola.dev',
            [
                'phases' => [],
                'tasks' => [
                    [
                        'id' => 123,
                        'task' => [
                            'componentId' => 'keboola.wr-snowflake-blob-storage',
                            'configId' => '123',
                        ],
                    ],
                ],
            ],
            [
                'phases' => [],
                'tasks' => [
                    [
                        'id' => 123,
                        'task' => [
                            'componentId' => 'keboola.wr-db-snowflake-gcs',
                            'configId' => '123',
                        ],
                    ],
                ],
            ],
            'snowflake',
        ];

        yield 'orchestrator-with-generic-component' => [
            'keboola.orchestrator',
            'https://connection.europe-west3.gcp.keboola.com',
            [
                'phases' => [],
                'tasks' => [
                    [
                        'id' => 123,
                        'task' => [
                            'componentId' => 'some-component',
                            'configId' => '123',
                        ],
                    ],
                ],
            ],
            [
                'phases' => [],
                'tasks' => [
                    [
                        'id' => 123,
                        'task' => [
                            'componentId' => 'some-component',
                            'configId' => '123',
                        ],
                    ],
                ],
            ],
            'snowflake',
        ];

        yield 'fake-orchestrator-with-stack-specific-component' => [
            'keboola.fake-orchestrator',
            'https://connection.us-east4.gcp.keboola.com',
            [
                'phases' => [],
                'tasks' => [
                    [
                        'id' => 123,
                        'task' => [
                            'componentId' => 'keboola.wr-db-snowflake',
                            'configId' => '123',
                        ],
                    ],
                ],
            ],
            [
                'phases' => [],
                'tasks' => [
                    [
                        'id' => 123,
                        'task' => [
                            'componentId' => 'keboola.wr-db-snowflake',
                            'configId' => '123',
                        ],
                    ],
                ],
            ],
            'snowflake',
        ];
    }
}
