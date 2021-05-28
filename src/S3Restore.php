<?php

declare(strict_types=1);

namespace Keboola\ProjectRestore;

use Keboola\StorageApi\Client as StorageApi;
use Aws\S3\S3Client;
use Keboola\Temp\Temp;
use Psr\Log\LoggerInterface;

class S3Restore extends Restore
{
    private S3Client $s3Client;

    private string $bucket;

    private string $path;

    public function __construct(
        StorageApi $sapiClient,
        S3Client $s3Client,
        string $bucket,
        string $path,
        ?LoggerInterface $logger = null
    ) {
        $this->s3Client = $s3Client;
        $this->bucket = $bucket;
        $this->path = $this->trimSourceBasePath($path);
        parent::__construct($logger, $sapiClient);
    }

    private function trimSourceBasePath(?string $targetBasePath = null): string
    {
        if (empty($targetBasePath) || $targetBasePath === '/') {
            return '';
        } else {
            return trim($targetBasePath, '/') . '/';
        }
    }

    /**
     * @return resource|string
     */
    protected function getDataFromStorage(string $filePath, bool $streamContent = true)
    {
        $tmp = new Temp();
        $tmp->initRunFolder();

        $targetFile = $tmp->createFile(uniqid());

        $this->copyFileFromStorage($filePath, $targetFile->getPathname());

        if (!$streamContent) {
            $file = fopen($targetFile->getPathname(), 'r');
            if (!$file) {
                throw new \Exception(sprintf(
                    'File "%s" does not exists.',
                    $filePath
                ));
            }
            return $file;
        } else {
            return (string) file_get_contents($targetFile->getPathname());
        }
    }

    protected function copyFileFromStorage(string $sourceFilePath, string $targetFilePath): void
    {
        $this->s3Client->getObject([
            'Bucket' => $this->bucket,
            'Key' => $this->path . $sourceFilePath,
            'SaveAs' => $targetFilePath,
        ]);
    }

    protected function listTableFiles(string $tableId): array
    {
        $iterator = $this->s3Client->getIterator('ListObjects', [
            'Bucket' => $this->bucket,
            'Prefix' => $this->path . str_replace('.', '/', $tableId) . '.',
        ]);

        return array_map(
            fn(array $v) => substr($v['Key'], strlen($this->path)),
            iterator_to_array($iterator)
        );
    }
}
