<?php

namespace VictorMacko\Repository\Exception;

/**
 * BatchWriteItem kept returning UnprocessedItems (usually throttling) after every retry.
 *
 * Batches are written in order, so the requests before these were written. getUnprocessedItems() holds every
 * request that was not: the leftovers of the batch that gave up plus the batches that were never sent. They are
 * in the SDK's marshalled form, so they can be passed back to batchWrite() later.
 */
class UnprocessedItemsException extends \RuntimeException
{
    /** @var string */
    private $tableName;

    /** @var list<array<string, mixed>> */
    private $unprocessedItems;

    /** @var int */
    private $attempts;

    /**
     * @param list<array<string, mixed>> $unprocessedItems
     */
    public function __construct(string $tableName, array $unprocessedItems, int $attempts)
    {
        parent::__construct(sprintf(
            'DynamoDB left %d item(s) unprocessed in "%s" after %d attempts',
            count($unprocessedItems),
            $tableName,
            $attempts
        ));
        $this->tableName = $tableName;
        $this->unprocessedItems = $unprocessedItems;
        $this->attempts = $attempts;
    }

    public function getTableName(): string
    {
        return $this->tableName;
    }

    /**
     * @return list<array<string, mixed>> PutRequest/DeleteRequest entries that were not written
     */
    public function getUnprocessedItems(): array
    {
        return $this->unprocessedItems;
    }

    public function getAttempts(): int
    {
        return $this->attempts;
    }
}
