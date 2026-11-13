<?php


namespace VictorMacko\Repository;


use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDb\Exception\DynamoDbException;
use Aws\DynamoDb\Marshaler;
use Aws\Result;
use Psr\Log\LoggerInterface;
use Symfony\Component\OptionsResolver\OptionsResolver;
use Symfony\Component\Serializer\Exception\NotEncodableValueException;
use Symfony\Component\Serializer\SerializerInterface;
use Symfony\Contracts\Service\Attribute\Required;
use VictorMacko\Repository\Exception\ConditionFailed;
use VictorMacko\Repository\Exception\ResultTooLarge;
use VictorMacko\Repository\Exception\UnencodableObjectsException;
use VictorMacko\Repository\Exception\UnprocessedItemsException;

/**
 * Base for a per-table repository. Every method is one visible DynamoDB request (batch writes add only the retries of
 * unprocessed items). Errors are not swallowed: anything other than "no item" or a failed condition is the SDK's
 * DynamoDbException, so callers can still branch on getAwsErrorCode().
 */
abstract class AbstractDynamoDbRepository
{
    /** BatchWriteItem accepts at most 25 put/delete requests per call. */
    private const BATCH_WRITE_SIZE = 25;

    private const BATCH_WRITE_MAX_ATTEMPTS = 8;

    private const BATCH_WRITE_BASE_DELAY_MS = 50;

    private const BATCH_WRITE_MAX_DELAY_MS = 5000;

    private const ON_UNENCODABLE_THROW_AFTER_WRITE = 'throw_after_write';

    private const ON_UNENCODABLE_THROW_BEFORE_WRITE = 'throw_before_write';

    /** @var DynamoDbClient */
    protected $dynamoDbClient;

    /** @var LoggerInterface */
    protected $logger;

    /** @var SerializerInterface */
    protected $serializer;

    /** @var Marshaler|null */
    private $sharedMarshaler;

    /**
     * @param DynamoDbClient $dynamoDbClient
     * @param LoggerInterface $logger
     * @param SerializerInterface $serializer
     */
    #[Required]
    public function setServices(
        DynamoDbClient $dynamoDbClient,
        LoggerInterface $logger,
        SerializerInterface $serializer
    ) {
        $this->dynamoDbClient = $dynamoDbClient;
        $this->logger = $logger;
        $this->serializer = $serializer;
    }

    /**
     * GetItem by primary key, denormalized into class_name.
     *
     * Returns null only when the table has no item with that key. Every DynamoDbException propagates: throttling
     * once the SDK's retries are spent, AccessDenied, ResourceNotFound (wrong table), ValidationException (e.g. a key
     * of the wrong type), timeouts. Before 1.1.0 these were all returned as null, indistinguishable from "not found".
     *
     * Options:
     * - table_name (string, required)
     * - key (array, required): the full primary key as native PHP values, e.g. ['clientId' => 'abc', 'id' => '42-abc']
     * - class_name (string, required)
     * - denormalize_context (array)
     * - consistent_read (bool, default false): a strongly consistent read, for a read straight after a write
     *   (e.g. the page a save redirects to). It costs twice as much, so only use it there.
     *
     * @throws DynamoDbException
     */
    public function getOneOrNullById(array $options)
    {
        $resolver = new OptionsResolver();
        $resolver->setDefaults(
            [
                'table_name' => null,
                'key' => null,
                'class_name' => null,
                'denormalize_context' => [],
                'consistent_read' => false,
            ]
        );
        $resolver->setAllowedTypes('table_name', 'string');
        $resolver->setAllowedTypes('key', 'array');
        $resolver->setAllowedTypes('class_name', 'string');
        $resolver->setAllowedTypes('denormalize_context', 'array');
        $resolver->setAllowedTypes('consistent_read', 'bool');
        $options = $resolver->resolve($options);

        $result = $this->dynamoDbClient->getItem(
            [
                'TableName' => $options['table_name'],
                'Key' => $this->marshaler()->marshalItem($options['key']),
                'ConsistentRead' => $options['consistent_read'],
            ]
        );

        if (!isset($result['Item'])) {
            return null;
        }

        return $this->serializer->denormalize(
            $this->marshaler()->unmarshalItem($result['Item']),
            $options['class_name'],
            null,
            $options['denormalize_context']
        );
    }

    /**
     * Maps the Items of ONE Query/Scan response. Each response stops at 1 MB (measured before any filter), so this
     * alone silently truncates a collection that has grown past one page.
     *
     * @deprecated since 1.1.0. Use queryAll() (follows every page, bounded) and denormalizeItems():
     *             $this->denormalizeItems($this->queryAll($params, 500), Thing::class)
     *             or follow LastEvaluatedKey yourself and call this once per page.
     */
    protected function convertResultsToObjectArray(Result $result, string $className, array $options = []): array
    {
        $resolver = new OptionsResolver();
        $resolver->setDefaults(
            [
                'denormalize_context' => [],
            ]
        );
        $resolver->setAllowedTypes('denormalize_context', 'array');
        $options = $resolver->resolve($options);

        $ret = [];
        foreach ($result['Items'] as $row) {
            $rowData = $this->marshaler()->unmarshalItem($row, false);
            $ret[] = $this->serializer->denormalize($rowData, $className, null, $options['denormalize_context']);
        }

        return $ret;
    }

    /**
     * Runs a Query and follows LastEvaluatedKey until DynamoDB reports no more pages: for deliberately reading all of
     * a small collection, e.g. one tenant's rows. It stops only when LastEvaluatedKey is absent, so a page that is
     * empty (after a FilterExpression) is not mistaken for the end.
     *
     * It is bounded. Finding more than $maxItems items throws ResultTooLarge instead of reading on without limit, so
     * a collection that has outgrown "read it all" fails visibly; page through it with Limit + ExclusiveStartKey
     * instead. A Limit in $params is the page size here, not a total.
     *
     * @param array $params the SDK's Query parameters, already marshalled (TableName, KeyConditionExpression, ...)
     *
     * @return list<array<string, mixed>> unmarshalled items, in the order DynamoDB returned them
     *
     * @throws ResultTooLarge
     * @throws DynamoDbException
     */
    protected function queryAll(array $params, int $maxItems = 1000): array
    {
        if ($maxItems < 1) {
            throw new \InvalidArgumentException('queryAll() needs a $maxItems of at least 1');
        }

        $items = [];
        foreach ($this->dynamoDbClient->getPaginator('Query', $params) as $page) {
            foreach ($page['Items'] ?? [] as $raw) {
                if (count($items) >= $maxItems) {
                    throw new ResultTooLarge((string) ($params['TableName'] ?? ''), $maxItems);
                }
                $items[] = $this->marshaler()->unmarshalItem($raw);
            }
        }

        return $items;
    }

    /**
     * Denormalizes unmarshalled items (e.g. from queryAll()) into objects.
     *
     * @param list<array<string, mixed>> $items
     *
     * @return list<object>
     */
    protected function denormalizeItems(array $items, string $className, array $denormalizeContext = []): array
    {
        $objects = [];
        foreach ($items as $item) {
            $objects[] = $this->serializer->denormalize($item, $className, null, $denormalizeContext);
        }

        return $objects;
    }

    /**
     * PutItem of the whole object (serialized to JSON). Without a condition this replaces any item with the same key.
     *
     * Options:
     * - table_name (string, required)
     * - serialize_context (array)
     * - condition_expression (string|null): e.g. 'attribute_not_exists(#id)' to create only, or '#v = :seen' for an
     *   optimistic version check. When it is not met, ConditionFailed is thrown and nothing is written.
     * - expression_attribute_names (array): e.g. ['#id' => 'id']
     * - expression_attribute_values (array): native PHP values, marshalled here, e.g. [':seen' => 3]
     *
     * @throws ConditionFailed only when a condition_expression was given
     * @throws DynamoDbException
     */
    protected function saveObject($object, array $options): void
    {
        $resolver = new OptionsResolver();
        $resolver->setDefaults(
            [
                'table_name' => null,
                'serialize_context' => [],
            ]
        );
        $resolver->setAllowedTypes('table_name', 'string');
        $resolver->setAllowedTypes('serialize_context', 'array');
        $this->configureConditionOptions($resolver);
        $options = $resolver->resolve($options);

        $json = $this->serializer->serialize($object, 'json', $options['serialize_context']);

        $this->writeItem(
            'putItem',
            [
                'TableName' => $options['table_name'],
                'Item' => $this->marshaler()->marshalJson($json),
            ] + $this->conditionParams($options)
        );
    }

    /**
     * Saves many objects with BatchWriteItem (see batchWrite()): duplicates collapsed, 25 per call, unprocessed items
     * retried with backoff, and an exception rather than silent loss when they still cannot be written.
     * Batch writes cannot carry conditions; they always replace any item with the same key.
     *
     * Options:
     * - table_name (string, required)
     * - serialize_context (array, default: ignore invalid UTF-8)
     * - key_attributes (list<string>|null): the table's partition (and sort) key attribute names, e.g.
     *   ['clientId', 'id']. With them, objects with the same key are collapsed, the last one winning; DynamoDB
     *   rejects a whole batch that names a key twice. Without them only identical objects are collapsed.
     * - on_unencodable ('throw_after_write' default | 'throw_before_write'): what to do when an object cannot be
     *   serialized. 'throw_after_write' saves the others and then throws UnencodableObjectsException listing the
     *   failures; 'throw_before_write' throws before anything is written. (Before 1.1.0 they were logged and skipped.)
     * - max_attempts (int, default 8), retry_base_delay_ms (int, default 50): see batchWrite()
     *
     * @throws UnencodableObjectsException
     * @throws UnprocessedItemsException
     * @throws DynamoDbException
     */
    protected function saveObjects(array $objects, array $options): void
    {
        $resolver = new OptionsResolver();
        $resolver->setDefaults(
            [
                'table_name' => null,
                'serialize_context' => ['json_encode_options' => JSON_INVALID_UTF8_IGNORE],
                'on_unencodable' => self::ON_UNENCODABLE_THROW_AFTER_WRITE,
            ]
        );
        $resolver->setAllowedTypes('table_name', 'string');
        $resolver->setAllowedTypes('serialize_context', 'array');
        $resolver->setAllowedValues(
            'on_unencodable',
            [self::ON_UNENCODABLE_THROW_AFTER_WRITE, self::ON_UNENCODABLE_THROW_BEFORE_WRITE]
        );
        $this->configureBatchWriteOptions($resolver);
        $options = $resolver->resolve($options);

        $requests = [];
        $failures = [];
        foreach ($objects as $index => $object) {
            try {
                $json = $this->serializer->serialize($object, 'json', $options['serialize_context']);
                $requests[] = ['PutRequest' => ['Item' => $this->marshaler()->marshalJson($json)]];
            } catch (NotEncodableValueException $e) {
                $failures[] = ['index' => $index, 'class' => get_debug_type($object), 'exception' => $e];
            }
        }

        if ($failures !== [] && $options['on_unencodable'] === self::ON_UNENCODABLE_THROW_BEFORE_WRITE) {
            throw new UnencodableObjectsException($options['table_name'], $failures, false);
        }

        $this->batchWrite(
            $options['table_name'],
            $requests,
            [
                'key_attributes' => $options['key_attributes'],
                'max_attempts' => $options['max_attempts'],
                'retry_base_delay_ms' => $options['retry_base_delay_ms'],
            ]
        );

        if ($failures !== []) {
            throw new UnencodableObjectsException($options['table_name'], $failures, $requests !== []);
        }
    }

    /**
     * BatchWriteItem for any number of PutRequest/DeleteRequest entries, already marshalled, to one table.
     *
     * - Requests for the same key are collapsed first, the last one winning (the same result as sending them one
     *   after another), because DynamoDB rejects a whole batch that names a key twice. Pass key_attributes to
     *   collapse by key; without them only identical requests are collapsed.
     * - Sent 25 per call.
     * - UnprocessedItems, which DynamoDB returns with HTTP 200 when it throttles part of a batch, are re-sent with
     *   capped exponential backoff and full jitter. If some are still unprocessed after max_attempts calls, an
     *   UnprocessedItemsException carries every request that was not written. Nothing is dropped silently.
     *
     * Batches are written in order and each one is finished before the next starts. BatchWriteItem is not a
     * transaction: when this throws, the requests before the failed batch have been written.
     *
     * Options:
     * - key_attributes (list<string>|null): the table's partition (and sort) key attribute names
     * - max_attempts (int, default 8): BatchWriteItem calls per batch of 25, retries included
     * - retry_base_delay_ms (int, default 50): the wait before retry n is random between 0 and
     *   min(5000, retry_base_delay_ms * 2^n) ms
     *
     * @param list<array<string, mixed>> $requests e.g. ['PutRequest' => ['Item' => $marshalledItem]] or
     *                                             ['DeleteRequest' => ['Key' => $marshalledKey]]
     *
     * @throws UnprocessedItemsException
     * @throws DynamoDbException
     */
    protected function batchWrite(string $table, array $requests, array $options = []): void
    {
        $resolver = new OptionsResolver();
        $this->configureBatchWriteOptions($resolver);
        $options = $resolver->resolve($options);

        $batches = array_chunk($this->collapseDuplicateRequests($requests, $options['key_attributes']), self::BATCH_WRITE_SIZE);

        foreach ($batches as $batchIndex => $pending) {
            for ($attempt = 1; ; ++$attempt) {
                $result = $this->dynamoDbClient->batchWriteItem(['RequestItems' => [$table => $pending]]);
                $pending = $result['UnprocessedItems'][$table] ?? [];
                if ($pending === []) {
                    break;
                }

                if ($attempt >= $options['max_attempts']) {
                    throw new UnprocessedItemsException(
                        $table,
                        array_merge($pending, ...array_slice($batches, $batchIndex + 1)),
                        $attempt
                    );
                }

                if ($this->logger !== null) {
                    $this->logger->warning(
                        'DynamoDB returned unprocessed items, retrying',
                        ['table' => $table, 'remaining' => count($pending), 'attempt' => $attempt]
                    );
                }
                $this->sleepBeforeBatchRetry($this->batchRetryDelayMicroseconds($attempt, $options['retry_base_delay_ms']));
            }
        }
    }

    /**
     * DeleteItem by primary key. Without a condition, deleting a key that does not exist succeeds silently.
     *
     * Options:
     * - table_name (string, required)
     * - key (array, required): the full primary key as native PHP values
     * - condition_expression (string|null): e.g. 'attribute_exists(#id)' to fail with ConditionFailed when the item
     *   does not exist
     * - expression_attribute_names (array), expression_attribute_values (array, native PHP values)
     *
     * @throws ConditionFailed only when a condition_expression was given
     * @throws DynamoDbException
     */
    protected function deleteByKey(array $options): void
    {
        $resolver = new OptionsResolver();
        $resolver->setDefaults(
            [
                'table_name' => null,
                'key' => null,
            ]
        );
        $resolver->setAllowedTypes('table_name', 'string');
        $resolver->setAllowedTypes('key', 'array');
        $this->configureConditionOptions($resolver);
        $options = $resolver->resolve($options);

        $this->writeItem(
            'deleteItem',
            [
                'TableName' => $options['table_name'],
                'Key' => $this->marshaler()->marshalItem($options['key']),
            ] + $this->conditionParams($options)
        );
    }

    /**
     * One Marshaler for the repository, created on first use.
     */
    protected function marshaler(): Marshaler
    {
        if ($this->sharedMarshaler === null) {
            $this->sharedMarshaler = new Marshaler();
        }

        return $this->sharedMarshaler;
    }

    /**
     * Waits between BatchWriteItem retries. Overridable so tests need not sleep.
     */
    protected function sleepBeforeBatchRetry(int $microseconds): void
    {
        if ($microseconds > 0) {
            usleep($microseconds);
        }
    }

    private function batchRetryDelayMicroseconds(int $attempt, int $baseDelayMs): int
    {
        $capMs = (int) min(self::BATCH_WRITE_MAX_DELAY_MS, $baseDelayMs * (2 ** $attempt));

        return random_int(0, $capMs) * 1000;
    }

    private function configureConditionOptions(OptionsResolver $resolver): void
    {
        $resolver->setDefaults(
            [
                'condition_expression' => null,
                'expression_attribute_names' => [],
                'expression_attribute_values' => [],
            ]
        );
        $resolver->setAllowedTypes('condition_expression', ['null', 'string']);
        $resolver->setAllowedTypes('expression_attribute_names', 'array');
        $resolver->setAllowedTypes('expression_attribute_values', 'array');
    }

    private function conditionParams(array $options): array
    {
        if ($options['condition_expression'] === null) {
            if ($options['expression_attribute_names'] !== [] || $options['expression_attribute_values'] !== []) {
                throw new \InvalidArgumentException(
                    'expression_attribute_names and expression_attribute_values are only used with a condition_expression'
                );
            }

            return [];
        }

        // DynamoDB rejects empty ExpressionAttributeNames/Values maps, so only send the ones that are used.
        $params = ['ConditionExpression' => $options['condition_expression']];
        if ($options['expression_attribute_names'] !== []) {
            $params['ExpressionAttributeNames'] = $options['expression_attribute_names'];
        }
        if ($options['expression_attribute_values'] !== []) {
            $params['ExpressionAttributeValues'] = $this->marshaler()->marshalItem($options['expression_attribute_values']);
        }

        return $params;
    }

    /**
     * PutItem/DeleteItem; a failed condition becomes ConditionFailed, any other error propagates unchanged.
     */
    private function writeItem(string $operation, array $params): void
    {
        try {
            $this->dynamoDbClient->{$operation}($params);
        } catch (DynamoDbException $e) {
            if (isset($params['ConditionExpression']) && $e->getAwsErrorCode() === 'ConditionalCheckFailedException') {
                throw new ConditionFailed($params['TableName'], $e);
            }

            throw $e;
        }
    }

    private function configureBatchWriteOptions(OptionsResolver $resolver): void
    {
        $resolver->setDefaults(
            [
                'key_attributes' => null,
                'max_attempts' => self::BATCH_WRITE_MAX_ATTEMPTS,
                'retry_base_delay_ms' => self::BATCH_WRITE_BASE_DELAY_MS,
            ]
        );
        $resolver->setAllowedTypes('key_attributes', ['null', 'string[]']);
        $resolver->setAllowedValues(
            'key_attributes',
            static function (?array $value): bool {
                return $value === null || in_array(count($value), [1, 2], true);
            }
        );
        $resolver->setAllowedTypes('max_attempts', 'int');
        $resolver->setAllowedValues(
            'max_attempts',
            static function (int $value): bool {
                return $value >= 1;
            }
        );
        $resolver->setAllowedTypes('retry_base_delay_ms', 'int');
        $resolver->setAllowedValues(
            'retry_base_delay_ms',
            static function (int $value): bool {
                return $value >= 0;
            }
        );
    }

    /**
     * @param list<array<string, mixed>> $requests
     * @param list<string>|null $keyAttributes
     *
     * @return list<array<string, mixed>>
     */
    private function collapseDuplicateRequests(array $requests, ?array $keyAttributes): array
    {
        $unique = [];
        foreach ($requests as $request) {
            // serialize(), not json_encode(): binary (B) values are not valid UTF-8.
            $id = $keyAttributes === null ? serialize($request) : $this->requestKeyId($request, $keyAttributes);
            $unique[$id] = $request;
        }

        return array_values($unique);
    }

    /**
     * @param list<string> $keyAttributes
     */
    private function requestKeyId(array $request, array $keyAttributes): string
    {
        $attributes = $request['PutRequest']['Item'] ?? $request['DeleteRequest']['Key'] ?? null;
        if (!is_array($attributes)) {
            throw new \InvalidArgumentException('A batch write request must be a PutRequest with an Item or a DeleteRequest with a Key');
        }

        $key = [];
        foreach ($keyAttributes as $name) {
            if (!array_key_exists($name, $attributes)) {
                throw new \InvalidArgumentException(sprintf('A batch write request has no "%s" key attribute', $name));
            }
            $key[$name] = $attributes[$name];
        }

        return serialize($key);
    }
}
