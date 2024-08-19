<?php

declare(strict_types=1);

namespace Keboola\ProjectRestore\Tests\StorageApi;

use Keboola\ProjectRestore\StorageApi\StackSpecificComponentIdTranslator;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;

class StackSpecificComponentIdTranslatorTest extends TestCase
{
    public function testTranslate(): void
    {
        $testHandler = new TestHandler();
        $logger = new Logger('test', [$testHandler]);

        $translator = new StackSpecificComponentIdTranslator(
            'connection.europe-west3.gcp.keboola.com',
            $logger
        );

        self::assertSame(
            'keboola.wr-db-snowflake-gcs',
            $translator->translate('keboola.wr-db-snowflake', 'bigquery')
        );
        self::assertSame(
            'keboola.wr-db-snowflake-gcs-s3',
            $translator->translate('keboola.wr-db-snowflake', 'snowflake')
        );

        $translator = new StackSpecificComponentIdTranslator(
            'connection.us-central1.gcp.keboola.dev',
            $logger
        );

        self::assertSame(
            'keboola.wr-db-snowflake-gcs',
            $translator->translate('keboola.wr-snowflake-blob-storage', 'bigquery')
        );
        self::assertSame(
            'keboola.wr-db-snowflake-gcs',
            $translator->translate('keboola.wr-snowflake-blob-storage', 'snowflake')
        );

        self::assertSame(
            'some-generic-component',
            $translator->translate('some-generic-component', 'bigquery')
        );

        $logRecords = $testHandler->getRecords();

        self::assertCount(4, $logRecords);

        self::assertSame(
            'Translated component ID from "keboola.wr-db-snowflake" to "keboola.wr-db-snowflake-gcs" '
            . 'for stack "connection.europe-west3.gcp.keboola.com".',
            array_shift($logRecords)['message']
        );

        self::assertSame(
            'Translated component ID from "keboola.wr-db-snowflake" to "keboola.wr-db-snowflake-gcs-s3" '
            . 'for stack "connection.europe-west3.gcp.keboola.com".',
            array_shift($logRecords)['message']
        );

        self::assertSame(
            'Translated component ID from "keboola.wr-snowflake-blob-storage" to "keboola.wr-db-snowflake-gcs" '
            . 'for stack "connection.us-central1.gcp.keboola.dev".',
            array_shift($logRecords)['message']
        );
    }

    public function testInvalidDestinationStack(): void
    {
        $testHandler = new TestHandler();
        $logger = new Logger('test', [$testHandler]);

        $translator = new StackSpecificComponentIdTranslator(
            'connection.north-europe.azure.keboola.com',
            $logger
        );

        self::assertSame(
            'keboola.wr-snowflake-blob-storage',
            $translator->translate('keboola.wr-snowflake-blob-storage', 'snowflake')
        );

        self::assertTrue($testHandler->hasWarningThatContains(
            'Component "keboola.wr-snowflake-blob-storage" is stack specific, but is not mapped '
            . 'for the destination stack "connection.north-europe.azure.keboola.com".'
        ));
    }
}
