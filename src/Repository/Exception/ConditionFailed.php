<?php

namespace VictorMacko\Repository\Exception;

use Aws\DynamoDb\Exception\DynamoDbException;

/**
 * A write's condition_expression evaluated to false, so DynamoDB did not write anything
 * (the SDK's ConditionalCheckFailedException, kept as the previous exception).
 *
 * Only thrown for writes that were given a condition_expression. Map it to a domain error in the repository:
 * "already exists" for attribute_not_exists(...), "not found" for attribute_exists(...), "changed by someone else"
 * for a version check.
 */
class ConditionFailed extends \RuntimeException
{
    /** @var string */
    private $tableName;

    public function __construct(string $tableName, DynamoDbException $previous)
    {
        parent::__construct(
            sprintf('The condition on a write to "%s" was not met: %s', $tableName, $previous->getAwsErrorMessage() ?? $previous->getMessage()),
            0,
            $previous
        );
        $this->tableName = $tableName;
    }

    public function getTableName(): string
    {
        return $this->tableName;
    }
}
