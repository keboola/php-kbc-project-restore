<?php

declare(strict_types=1);

namespace Keboola\ProjectRestore;

use Keboola\StorageApi\Client as StorageApi;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Blob\Models\Blob;
use MicrosoftAzure\Storage\Blob\Models\ListBlobsOptions;
use Psr\Log\LoggerInterface;

class AbsRestore extends Restore
{
    private BlobRestProxy $absClient;

    private string $container;

    private ?string $blobPrefix = null;

    public function __construct(
        StorageApi $sapiClient,
        BlobRestProxy $absClient,
        string $container,
        ?LoggerInterface $logger = null
    ) {
        $this->absClient = $absClient;
        preg_match('/([^\/]+)\/?(.+)?/', $container, $match);
        $this->container = $match[1];
        if (isset($match[2])) {
            $this->blobPrefix = (string) $match[2];
        }

        parent::__construct($logger, $sapiClient);
    }

    /**
     * @return resource|string
     */
    protected function getDataFromStorage(string $filePath, bool $useString = true)
    {
        $container = $this->container;
        if ($this->blobPrefix) {
            $container = sprintf('%s/%s', $this->container, $this->blobPrefix);
        }
        $configFileContent = $this->absClient->getBlob(
            $container,
            $filePath
        );
        if (!$useString) {
            return $configFileContent->getContentStream();
        } else {
            return (string) stream_get_contents($configFileContent->getContentStream());
        }
    }

    protected function copyFileFromStorage(string $sourceFilePath, string $targetFilePath): void
    {
        file_put_contents($targetFilePath, $this->getDataFromStorage($sourceFilePath, false));
    }

    protected function listTableFiles(string $tableId): array
    {
        $options = new ListBlobsOptions();
        $options->setPrefix($this->blobPrefix . '/' . str_replace('.', '/', $tableId) . '.');

        $blobs = $this->absClient->listBlobs($this->container, $options);

        return array_map(
            fn(Blob $v) => substr($v->getName(), strlen($this->blobPrefix . '/')),
            $blobs->getBlobs()
        );
    }
}
