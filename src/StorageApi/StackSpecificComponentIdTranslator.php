<?php

declare(strict_types=1);

namespace Keboola\ProjectRestore\StorageApi;

use Google\Api\Backend;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\StorageApi\Client;
use Psr\Log\LoggerInterface;

class StackSpecificComponentIdTranslator
{
    private string $destinationStack;
    private LoggerInterface $logger;

    public function __construct(
        string $destinationStack,
        LoggerInterface $logger,
    ) {
        $this->destinationStack = $destinationStack;
        $this->logger = $logger;
    }

    private const STACK_SPECIFIC_COMPONENTS = [
        'keboola.wr-db-snowflake'           => [
            'connection.europe-west3.gcp.keboola.com' => 'keboola.wr-db-snowflake-gcs',
            'connection.us-east4.gcp.keboola.com'     => 'keboola.wr-db-snowflake-gcs',
            'connection.europe-west2.gcp.keboola.com' => 'keboola.wr-db-snowflake-gcs',
            'connection.us-central1.gcp.keboola.dev'  => 'keboola.wr-db-snowflake-gcs',
        ],
        'keboola.wr-snowflake-blob-storage' => [
            'connection.europe-west3.gcp.keboola.com' => 'keboola.wr-db-snowflake-gcs',
            'connection.us-east4.gcp.keboola.com'     => 'keboola.wr-db-snowflake-gcs',
            'connection.europe-west2.gcp.keboola.com' => 'keboola.wr-db-snowflake-gcs',
            'connection.us-central1.gcp.keboola.dev'  => 'keboola.wr-db-snowflake-gcs',
        ],
    ];

    private const STACK_ALLOW_GCS_S3_WRITER = [
        'connection.europe-west3.gcp.keboola.com',
        'connection.us-east4.gcp.keboola.com',
    ];

    public function translate(string $componentId, string $backendType): string
    {
        if (!array_key_exists($componentId, self::STACK_SPECIFIC_COMPONENTS)) {
            return $componentId;
        }

        if (!array_key_exists($this->destinationStack, self::STACK_SPECIFIC_COMPONENTS[$componentId])) {
            $this->logger->warning(sprintf(
                'Component "%s" is stack specific, but is not mapped for the destination stack "%s".',
                $componentId,
                $this->destinationStack,
            ));
            return $componentId;
        }

        $translatedComponentId = self::STACK_SPECIFIC_COMPONENTS[$componentId][$this->destinationStack];
        if ($translatedComponentId === 'keboola.wr-db-snowflake-gcs' &&
            $backendType === 'snowflake' &&
            in_array($this->destinationStack, self::STACK_ALLOW_GCS_S3_WRITER)
        ) {
            $translatedComponentId .= '-s3';
        }

        $this->logger->info(sprintf(
            'Translated component ID from "%s" to "%s" for stack "%s".',
            $componentId,
            $translatedComponentId,
            $this->destinationStack,
        ));
        return $translatedComponentId;
    }
}
