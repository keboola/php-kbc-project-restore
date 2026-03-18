<?php

declare(strict_types=1);

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\Temp\Temp;

require __DIR__ . '/../vendor/autoload.php';

/** @var array{sapiUrl: string, sapiToken: string, bucketId: string, tableName: string, columns: string[], primaryKey: string[], isTyped: bool, displayName?: string, tableDefinition?: array<string, mixed>} $input */
$input = json_decode((string) stream_get_contents(STDIN), true);

$client = new Client(['url' => $input['sapiUrl'], 'token' => $input['sapiToken']]);

try {
    if ($input['isTyped']) {
        $tableId = $client->createTableDefinition($input['bucketId'], $input['tableDefinition'] ?? []);
    } else {
        $tmp = new Temp();
        $headerFile = new CsvFile($tmp->createFile(sprintf('%s.header.csv', $input['tableName']))->getPathname());
        $headerFile->writeRow($input['columns']);

        $tableId = $client->createTableAsync(
            $input['bucketId'],
            $input['tableName'],
            $headerFile,
            ['primaryKey' => implode(',', $input['primaryKey'])],
        );
    }

    if (isset($input['displayName'])) {
        $client->updateTable($tableId, ['displayName' => $input['displayName']]);
    }

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
