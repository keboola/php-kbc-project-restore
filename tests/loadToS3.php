<?php

declare(strict_types=1);

date_default_timezone_set('Europe/Prague');
ini_set('display_errors', '1');
error_reporting(E_ALL);

$basedir = dirname(__FILE__);

require_once $basedir . '/bootstrap.php';

echo "Loading fixtures to S3\n";

// delete from S3
$s3Client = new \Aws\S3\S3Client([
    'version' => 'latest',
    'region' => getenv('TEST_AWS_REGION'),
    'credentials' => [
        'key' => getenv('TEST_AWS_ACCESS_KEY_ID'),
        'secret' => getenv('TEST_AWS_SECRET_ACCESS_KEY'),
    ],
]);
$s3Client->deleteMatchingObjects(getenv('TEST_AWS_S3_BUCKET'), '*');

// Where the files will be source from
$source = $basedir . '/data/backups';

// Where the files will be transferred to
$dest = 's3://' . getenv('TEST_AWS_S3_BUCKET') . '/';

// Create a transfer object.
$manager = new \Aws\S3\Transfer($s3Client, $source, $dest, []);

// Perform the transfer synchronously.
$manager->transfer();
