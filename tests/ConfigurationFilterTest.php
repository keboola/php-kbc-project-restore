<?php

declare(strict_types=1);

namespace Keboola\ProjectRestore\Tests;

use Keboola\ProjectRestore\StorageApi\ConfigurationFilter;

class ConfigurationFilterTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @dataProvider oauthFilterData
     * @param array $inConfigData
     * @param array $expectedOutConfigData
     */
    public function testOauthFilter(array $inConfigData, array $expectedOutConfigData): void
    {
        $filteredConfigData = ConfigurationFilter::removeOauthAuthorization(
            json_decode(json_encode($inConfigData))
        );
        $this->assertEquals(json_decode(json_encode($expectedOutConfigData)), $filteredConfigData);
    }

    public function oauthFilterData(): array
    {
        return [
            'with-oauth' => [
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
            ],
            'without-oauth' => [
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
            ],
            'with-weird-oauth' => [
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
            ],
            'with-injection-oauth' => [
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
            ],
        ];
    }
}
