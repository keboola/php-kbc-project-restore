<?php

declare(strict_types=1);

use Keboola\ProjectRestore\Tests\AbsRestoreTest;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Blob\Models\Container;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;
use Keboola\Temp\Temp;
use Keboola\Csv\CsvFile;

date_default_timezone_set('Europe/Prague');
ini_set('display_errors', '1');
error_reporting(E_ALL);

$basedir = dirname(__FILE__);

require_once $basedir . '/bootstrap.php';

echo 'Loading fixtures to ABS' . PHP_EOL;

$absClient = BlobRestProxy::createBlobService(sprintf(
    'DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=core.windows.net',
    (string) getenv('TEST_AZURE_ACCOUNT_NAME'),
    (string) getenv('TEST_AZURE_ACCOUNT_KEY')
));

echo 'Cleanup files in ABS' . PHP_EOL;
$containers = $absClient->listContainers();
$listContainers = array_map(fn(Container $v) => $v->getName(), $containers->getContainers());

if (!in_array((string) getenv('TEST_AZURE_CONTAINER_NAME'), $listContainers)) {
    $absClient->createContainer((string) getenv('TEST_AZURE_CONTAINER_NAME'));
}

$blobs = $absClient->listBlobs((string) getenv('TEST_AZURE_CONTAINER_NAME'));
foreach ($blobs->getBlobs() as $blob) {
    $absClient->deleteBlob((string) getenv('TEST_AZURE_CONTAINER_NAME'), $blob->getName());
}

echo 'Copying new files into ABS' . PHP_EOL;
$finder = new Finder();
$files = $finder->in($basedir . '/data/backups')->files();
foreach ($files as $file) {
    $fopen = fopen($file->getPathname(), 'r');
    if (!$fopen) {
        continue;
    }
    $absClient->createBlockBlob(
        (string) getenv('TEST_AZURE_CONTAINER_NAME'),
        $file->getRelativePathname(),
        $fopen
    );
}

// generate a lot of table slices
$system = new Filesystem();

$temp = new Temp('loadToAbs');
$temp->initRunFolder();

$tablesPath = sprintf('%s/table-%s-slices', $temp->getTmpFolder(), AbsRestoreTest::TEST_ITERATOR_SLICES_COUNT);
$slicesPath = $tablesPath . '/in/c-bucket';

$system->mkdir($tablesPath);

$system->mirror($basedir . '/data/backups/table-multiple-slices', $tablesPath, null, [
    'override' => true,
    'delete' => true,
]);

$system->remove((new Finder())->files()->in($slicesPath)->getIterator());

for ($i = 0; $i < AbsRestoreTest::TEST_ITERATOR_SLICES_COUNT; $i++) {
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

echo 'Copying new slices to ABS' . PHP_EOL;

$source = $temp->getTmpFolder();
$finder = new Finder();
$files = $finder->in($source)->files();
foreach ($files as $file) {
    $fopen = fopen($file->getPathname(), 'r');
    if (!$fopen) {
        continue;
    }
    $absClient->createBlockBlob(
        (string) getenv('TEST_AZURE_CONTAINER_NAME'),
        $file->getRelativePathname(),
        $fopen
    );
}

echo 'Fixtures load complete' . PHP_EOL;
