<?php

declare(strict_types=1);

use Keboola\Csv\CsvFile;
use Keboola\ProjectRestore\Tests\AbsRestoreTest;
use Keboola\Temp\Temp;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Blob\Models\Container;
use MicrosoftAzure\Storage\Blob\Models\ListBlobsOptions;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;

date_default_timezone_set('Europe/Prague');
ini_set('display_errors', '1');
error_reporting(E_ALL);

$basedir = dirname(__FILE__);

require_once $basedir . '/bootstrap.php';

echo 'Loading fixtures to ABS' . PHP_EOL;

$absClient = BlobRestProxy::createBlobService(sprintf(
    'DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=core.windows.net',
    (string) getenv('TEST_AZURE_ACCOUNT_NAME'),
    (string) getenv('TEST_AZURE_ACCOUNT_KEY'),
));

echo 'Cleanup files in ABS' . PHP_EOL;
$containers = $absClient->listContainers();
$listContainers = array_map(fn(Container $v) => $v->getName(), $containers->getContainers());

echo 'Copying new files into ABS' . PHP_EOL;
$finder = new Finder();
$dirs = $finder->depth(0)->in($basedir . '/data/backups')->directories();

foreach ($dirs as $dir) {
    $container = getenv('TEST_AZURE_CONTAINER_NAME') . '-' . $dir->getRelativePathname();
    if (!in_array($container, $listContainers)) {
        $absClient->createContainer($container);
    }

    $options = new ListBlobsOptions();
    $options->setPrefix('');
    $blobs = $absClient->listBlobs($container, $options);
    foreach ($blobs->getBlobs() as $blob) {
        $absClient->deleteBlob($container, $blob->getName());
    }

    $finder = new Finder();
    $files = $finder->in($dir->getPathname())->files();
    foreach ($files as $file) {
        $fopen = fopen($file->getPathname(), 'r');
        if (!$fopen) {
            continue;
        }
        $absClient->createBlockBlob(
            $container,
            $file->getRelativePathname(),
            $fopen,
        );
    }
}


// generate a lot of table slices
$system = new Filesystem();

$temp = new Temp('loadToAbs');

$slicesPath = $temp->getTmpFolder() . '/in/c-bucket';

$system->mkdir($temp->getTmpFolder());

$system->mirror($basedir . '/data/backups/table-multiple-slices', $temp->getTmpFolder(), null, [
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

$container = sprintf(
    '%s-table-%s-slices',
    getenv('TEST_AZURE_CONTAINER_NAME'),
    AbsRestoreTest::TEST_ITERATOR_SLICES_COUNT,
);
if (!in_array($container, $listContainers)) {
    $absClient->createContainer($container);
}

$options = new ListBlobsOptions();
$options->setPrefix('');
$blobs = $absClient->listBlobs($container, $options);
foreach ($blobs->getBlobs() as $blob) {
    $absClient->deleteBlob($container, $blob->getName());
}

foreach ($files as $file) {
    $fopen = fopen($file->getPathname(), 'r');
    if (!$fopen) {
        continue;
    }
    $absClient->createBlockBlob(
        $container,
        $file->getRelativePathname(),
        $fopen,
    );
}

echo 'Fixtures load complete' . PHP_EOL;
