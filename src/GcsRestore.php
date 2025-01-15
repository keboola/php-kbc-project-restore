<?php

declare(strict_types=1);

namespace Keboola\ProjectRestore;

use Exception;
use Keboola\StorageApi\Client as StorageApi;
use Keboola\Temp\Temp;
use Psr\Log\LoggerInterface;

class GcsRestore extends Restore
{
    public function __construct(
        StorageApi $sapiClient,
        readonly array $listFiles,
        ?LoggerInterface $logger = null,
    ) {
        parent::__construct($sapiClient, $logger);
    }

    /**
     * @return resource|string
     */
    protected function getDataFromStorage(string $filePath, bool $useString = true)
    {
        $tmp = new Temp();

        $targetFile = $tmp->createFile(uniqid());

        $this->copyFileFromStorage($filePath, $targetFile->getPathname());

        if (!$useString) {
            $file = fopen($targetFile->getPathname(), 'r');
            if (!$file) {
                throw new Exception(sprintf(
                    'File "%s" does not exists.',
                    $filePath,
                ));
            }
            return $file;
        } else {
            return (string) file_get_contents($targetFile->getPathname());
        }
    }

    protected function copyFileFromStorage(string $sourceFilePath, string $targetFilePath): void
    {
        $explodedSourceFilePath = explode('/', $sourceFilePath);
        $sourceUrl = $this->findUrl($this->listFiles, $explodedSourceFilePath);
        copy($sourceUrl, $targetFilePath);
    }

    protected function listComponentConfigurationsFiles(string $filePath): array
    {
        $explodedPath = explode('/', $filePath);
        array_shift($explodedPath);

        $actualList = $this->listFiles['configurations'];
        return $this->findPartOfTree($actualList, $explodedPath);
    }

    protected function listTableFiles(string $tableId): array
    {
        $explodedTableId = explode('.', $tableId);
        $tablePath = $explodedTableId;
        array_pop($tablePath);

        $actualList = $this->findPartOfTree($this->listFiles, $explodedTableId);

        $table = end($explodedTableId);

        $parts = [];
        foreach ($actualList as $fileName => $url) {
            if (str_starts_with($fileName, $table . '.')) {
                $parts[] = implode('/', $tablePath) . '/' . $fileName;
            }
        }
        return $parts;
    }

    public function findPartOfTree(array $list, array $findPath): array
    {
        return (array) $this->findInArray($list, $findPath);
    }

    private function findUrl(array $list, array $findPath): string
    {
        $result = $this->findInArray($list, $findPath);

        if (is_string($result)) {
            return $result;
        }

        throw new Exception(sprintf(
            'File "%s" does not exists.',
            implode('/', $findPath),
        ));
    }

    private function findInArray(array $list, array $findPath): string|array
    {
        while (count($findPath) > 0) {
            $current = current($findPath);
            if (!isset($list[$current])) {
                break;
            }
            $list = $list[$current];
            array_shift($findPath);
        }
        return $list;
    }
}
