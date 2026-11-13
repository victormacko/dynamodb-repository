<?php

namespace VictorMacko\Repository\Exception;

/**
 * queryAll() found more items than its $maxItems bound.
 *
 * The collection has outgrown "read all of it in one request": page through it with a cursor
 * (Limit + ExclusiveStartKey), or raise the bound deliberately.
 */
class ResultTooLarge extends \RuntimeException
{
    /** @var string */
    private $tableName;

    /** @var int */
    private $maxItems;

    public function __construct(string $tableName, int $maxItems)
    {
        parent::__construct(sprintf('Query on "%s" matched more than %d items', $tableName, $maxItems));
        $this->tableName = $tableName;
        $this->maxItems = $maxItems;
    }

    public function getTableName(): string
    {
        return $this->tableName;
    }

    public function getMaxItems(): int
    {
        return $this->maxItems;
    }
}
