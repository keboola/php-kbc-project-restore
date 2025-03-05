<?php

declare(strict_types=1);

namespace Keboola\ProjectRestore\StorageApi;

use Psr\Log\LoggerInterface;
use stdClass;

class ConfigurationCorrector
{
    private StackSpecificComponentIdTranslator $componentIdTranslator;

    private bool $disableOrchestrations;

    public function __construct(
        string $destinationApiUrl,
        LoggerInterface $logger,
        bool $disableOrchestrations
    ) {
        $destinationStack = self::getStackFromUrl($destinationApiUrl);

        $this->componentIdTranslator = new StackSpecificComponentIdTranslator(
            $destinationStack,
            $logger,
        );

        $this->disableOrchestrations = $disableOrchestrations;
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
        if ($this->disableOrchestrations) {
            $configuration->isDisabled = $this->disableOrchestrations;
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
