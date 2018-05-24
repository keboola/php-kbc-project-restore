<?php

declare(strict_types=1);

namespace Keboola\ProjectRestore\StorageApi;

class ConfigurationFilter
{

    public static function removeOauthAuthorization(array $configuration): array
    {
        if (isset($configuration['authorization']['oauth_api']['id'])) {
            unset($configuration['authorization']['oauth_api']['id']);
            return $configuration;
        }
        return $configuration;
    }
}
