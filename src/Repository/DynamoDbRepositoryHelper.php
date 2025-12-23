<?php

namespace VictorMacko\Repository;

use Aws\Result;

/**
 * Class DynamoDbRepositoryHelper
 * Exposes protected methods in repository class -- that class will shortly be migrated here.
 *
 * @package VictorMacko\Repository
 */
class DynamoDbRepositoryHelper extends AbstractDynamoDbRepository
{
    public function convertResultsToObjectArray(Result $result, string $className, array $options = []): array
    {
        return parent::convertResultsToObjectArray($result, $className, $options);
    }

    public function deleteByKey(array $options): void
    {
        parent::deleteByKey($options);
    }

    public function saveObject($object, array $options): void
    {
        parent::saveObject($object, $options);
    }
}