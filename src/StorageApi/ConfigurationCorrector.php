<?php

declare(strict_types=1);

namespace Keboola\ProjectRestore\StorageApi;

use Psr\Log\LoggerInterface;
use stdClass;

class ConfigurationCorrector
{
    private StackSpecificComponentIdTranslator $componentIdTranslator;

    public function __construct(
        string $destinationApiUrl,
        LoggerInterface $logger
    ) {
        $destinationStack = self::getStackFromUrl($destinationApiUrl);

        $this->componentIdTranslator = new StackSpecificComponentIdTranslator(
            $destinationStack,
            $logger
        );
    }

    public function correct(string $componentId, stdClass $configuration): stdClass
    {
        if ($componentId === 'keboola.orchestrator') {
            $configuration = $this->correctOrchestratorConfiguration($configuration);
        }

        $configuration = $this->removeOauthAuthorization($configuration);

        return $configuration;
    }

    private function correctOrchestratorConfiguration(stdClass $configuration): stdClass
    {
        foreach ($configuration->tasks ?? [] as $task) {
            $componentId = &$task->task->componentId;
            $componentId = $this->componentIdTranslator->translate($componentId);
        }

        return $configuration;
    }

    private function removeOauthAuthorization(stdClass $configuration): stdClass
    {
        if (isset($configuration->authorization->oauth_api->id)) {
            unset($configuration->authorization->oauth_api->id);
        }
        return $configuration;
    }

    private static function getStackFromUrl(string $url): string
    {
        return parse_url($url)['host'] ?? '';
    }
}
