<?php

declare(strict_types=1);

namespace Keboola\ProjectRestore\StorageApi;

use stdClass;

class ConfigurationFilter
{

    public static function removeOauthAuthorization(stdClass $configuration): stdClass
    {
        if (isset($configuration->authorization->oauth_api->id)) {
            unset($configuration->authorization->oauth_api->id);
            return $configuration;
        }
        return $configuration;
    }
}
