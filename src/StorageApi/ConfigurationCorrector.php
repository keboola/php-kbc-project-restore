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
        LoggerInterface $logger,
    ) {
        $destinationStack = self::getStackFromUrl($destinationApiUrl);

        $this->componentIdTranslator = new StackSpecificComponentIdTranslator(
            $destinationStack,
            $logger,
        );
    }

    public function correct(string $componentId, stdClass $configuration, string $backendType): stdClass
    {
        if ($componentId === 'keboola.orchestrator') {
            $configuration = $this->correctOrchestratorConfiguration($configuration, $backendType);
        }
        if ($componentId === 'keboola.data-apps') {
            $configuration = $this->removeDataAppId($configuration);
        }

        $configuration = $this->removeOauthAuthorization($configuration);

        return $configuration;
    }

    private function correctOrchestratorConfiguration(stdClass $configuration, string $backendType): stdClass
    {
        foreach ($configuration->tasks ?? [] as $task) {
            $task->task->componentId = $this->componentIdTranslator->translate($task->task->componentId, $backendType);
        }
        $configuration->isDisabled = true;
        return $configuration;
    }

    private function removeOauthAuthorization(stdClass $configuration): stdClass
    {
        if (isset($configuration->authorization->oauth_api->id)) {
            unset($configuration->authorization->oauth_api->id);
        }
        return $configuration;
    }

    private function removeDataAppId(stdClass $configuration): stdClass
    {
        if (isset($configuration->parameters->id)) {
            unset($configuration->parameters->id);
        }
        return $configuration;
    }

    private static function getStackFromUrl(string $url): string
    {
        $parsedUrl = parse_url($url);
        return is_array($parsedUrl) && isset($parsedUrl['host']) ? $parsedUrl['host'] : '';
    }
}
