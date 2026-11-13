<?php

declare(strict_types=1);

namespace VictorMacko\Tests\Fixtures;

use Aws\Result;
use VictorMacko\Repository\AbstractDynamoDbRepository;

/**
 * Makes the protected API callable from tests, the way a consumer's subclass would use it, and records the
 * BatchWriteItem retry waits instead of sleeping.
 */
final class TestRepository extends AbstractDynamoDbRepository
{
    /** @var list<int> */
    public array $sleeps = [];

    public function convertResultsToObjectArray(Result $result, string $className, array $options = []): array
    {
        return parent::convertResultsToObjectArray($result, $className, $options);
    }

    public function queryAll(array $params, int $maxItems = 1000): array
    {
        return parent::queryAll($params, $maxItems);
    }

    public function denormalizeItems(array $items, string $className, array $denormalizeContext = []): array
    {
        return parent::denormalizeItems($items, $className, $denormalizeContext);
    }

    public function saveObject($object, array $options): void
    {
        parent::saveObject($object, $options);
    }

    public function saveObjects(array $objects, array $options): void
    {
        parent::saveObjects($objects, $options);
    }

    public function batchWrite(string $table, array $requests, array $options = []): void
    {
        parent::batchWrite($table, $requests, $options);
    }

    public function deleteByKey(array $options): void
    {
        parent::deleteByKey($options);
    }

    protected function sleepBeforeBatchRetry(int $microseconds): void
    {
        $this->sleeps[] = $microseconds;
    }
}
