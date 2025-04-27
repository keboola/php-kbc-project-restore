<?php

declare(strict_types=1);

use Aws\S3\S3Client;
use Aws\S3\Transfer;
use Keboola\Csv\CsvFile;
use Keboola\ProjectRestore\Tests\S3RestoreTest;
use Keboola\Temp\Temp;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;

date_default_timezone_set('Europe/Prague');
ini_set('display_errors', '1');
error_reporting(E_ALL);

$basedir = dirname(__FILE__);

require_once $basedir . '/../bootstrap.php';

echo 'Loading fixtures to S3' . PHP_EOL;

$s3Client = new S3Client([
    'version' => 'latest',
    'region' => getenv('TEST_AWS_REGION'),
    'credentials' => [
        'key' => getenv('TEST_AWS_ACCESS_KEY_ID'),
        'secret' => getenv('TEST_AWS_SECRET_ACCESS_KEY'),
    ],
]);

// delete from S3
echo 'Cleanup files in S3' . PHP_EOL;
$s3Client->deleteMatchingObjects((string) getenv('TEST_AWS_S3_BUCKET'), '*');

// copy new files
$source = $basedir . '/data';
$dest = 's3://' . getenv('TEST_AWS_S3_BUCKET') . '/';

echo 'Copying new files into S3' . PHP_EOL;
$manager = new Transfer($s3Client, $source, $dest, []);
$manager->transfer();

// generate a lot of table slices
$system = new Filesystem();

$temp = new Temp('loadToS3');

$tablesPath = sprintf('%s/table-%s-slices', $temp->getTmpFolder(), S3RestoreTest::TEST_ITERATOR_SLICES_COUNT);
$slicesPath = $tablesPath . '/in/c-bucket';

$system->mkdir($tablesPath);

$system->mirror($basedir . '/data/table-multiple-slices', $tablesPath, null, [
    'override' => true,
    'delete' => true,
]);

$system->remove((new Finder())->files()->in($slicesPath)->getIterator());

for ($i = 0; $i < S3RestoreTest::TEST_ITERATOR_SLICES_COUNT; $i++) {
    $part = str_pad((string) $i, 5, '0', STR_PAD_LEFT);

    $csv = new CsvFile(sprintf('%s/Account.part_%s.csv', $slicesPath, $part));
    $csv->writeRow([
        $part,
        uniqid('', true),
    ]);

    unset($csv);
    echo '.';
}

echo PHP_EOL;
echo 'Slices count: ' . $i . PHP_EOL;

echo 'Copying new slices to S3' . PHP_EOL;

$source = $temp->getTmpFolder();
$dest = 's3://' . getenv('TEST_AWS_S3_BUCKET') . '/';

$manager = new Transfer($s3Client, $source, $dest, []);
$manager->transfer();


echo 'Fixtures load complete' . PHP_EOL;
