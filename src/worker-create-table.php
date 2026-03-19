<?php

declare(strict_types=1);

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\Temp\Temp;

$raw = json_decode((string) stream_get_contents(STDIN), true);

/** @var array{autoloadPath: string, sapiUrl: string, sapiToken: string, runId: string|null, bucketId: string, tableName: string, columns: string[], primaryKey: string[], isTyped: bool, displayName: string, tableDefinition?: array<string, mixed>} $raw */
require $raw['autoloadPath'];

$client = new Client(['url' => $raw['sapiUrl'], 'token' => $raw['sapiToken']]);
if ($raw['runId'] !== null) {
    $client->setRunId($raw['runId']);
}

try {
    if ($raw['isTyped']) {
        $tableId = $client->createTableDefinition($raw['bucketId'], $raw['tableDefinition'] ?? []);
    } else {
        $tmp = new Temp();
        $headerFile = new CsvFile($tmp->createFile(sprintf('%s.header.csv', $raw['tableName']))->getPathname());
        $headerFile->writeRow($raw['columns']);

        $tableId = $client->createTableAsync(
            $raw['bucketId'],
            $raw['tableName'],
            $headerFile,
            ['primaryKey' => implode(',', $raw['primaryKey'])],
        );
    }

    $client->updateTable($tableId, ['displayName' => $raw['displayName']]);

    echo json_encode(['tableId' => $tableId, 'error' => null]);
    exit(0);
} catch (ClientException $e) {
    $isNullablePkError = $e->getCode() === 400
        && (bool) preg_match('/Primary keys columns must be set nullable false/', $e->getMessage());

    echo json_encode([
        'tableId' => null,
        'error' => $e->getMessage(),
        'code' => $e->getCode(),
        'isNullablePkError' => $isNullablePkError,
        'isClientException' => true,
    ]);
    exit(1);
} catch (Throwable $e) {
    echo json_encode([
        'tableId' => null,
        'error' => $e->getMessage(),
        'code' => 0,
        'isNullablePkError' => false,
        'isClientException' => false,
    ]);
    exit(1);
}
