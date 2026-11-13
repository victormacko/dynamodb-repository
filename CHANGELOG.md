# Changelog

## 1.1.0 (unreleased)

No method signature has changed and every new option is optional. Two behaviour changes are bug fixes that callers may notice; read **Upgrading** first. Finding numbers refer to `DYNAMODB-REVIEW.md`.

### Fixed

- **`getOneOrNullById()` no longer turns DynamoDB errors into `null` (F1, AP-04).** It returns `null` only when the response has no `Item`. Throttling (once the SDK's retries are spent), `AccessDeniedException`, `ResourceNotFoundException`, `ValidationException` and network errors now propagate as `Aws\DynamoDb\Exception\DynamoDbException`.
- **`saveObjects()` no longer loses writes (F2, AP-03).**
  - `UnprocessedItems` are retried, up to 8 calls per batch of 25, with capped exponential backoff and full jitter (a random wait of up to 50 ms × 2ⁿ, at most 5 s). If items are still unprocessed, `Exception\UnprocessedItemsException` carries every request that was not written.
  - Duplicates are collapsed before batching, because DynamoDB rejects a whole batch that names a key twice. Identical objects are always collapsed. Objects with the same key are collapsed when you pass `key_attributes` (the last one wins).
  - An object the serializer cannot encode is no longer logged and skipped. `Exception\UnencodableObjectsException` is thrown after the other objects are saved (the default), or before anything is written with `'on_unencodable' => 'throw_before_write'`.

### Added

- `getOneOrNullById()` option `consistent_read` (default `false`) for a read straight after a write (F5, AP-13).
- `saveObject()` and `deleteByKey()` options `condition_expression`, `expression_attribute_names` and `expression_attribute_values` (native PHP values). A failed condition throws `Exception\ConditionFailed`, with the SDK exception as previous (F4, AP-06).
- `queryAll(array $params, int $maxItems = 1000): array` follows `LastEvaluatedKey` until it is absent and returns unmarshalled items. It throws `Exception\ResultTooLarge` instead of reading past `$maxItems` (F3, AP-01).
- `denormalizeItems(array $items, string $className, array $denormalizeContext = [])`.
- `batchWrite(string $table, array $requests, array $options = [])` for marshalled PutRequest/DeleteRequest entries, with the same collapsing, chunking and retries. Its name and first two arguments match the `microservice-holidays` fork.
- `marshaler()`: one shared Marshaler per repository (F7).
- `saveObjects()` options `key_attributes`, `on_unencodable`, `max_attempts` and `retry_base_delay_ms`.
- A PHPUnit suite on `Aws\MockHandler`: run `vendor/bin/phpunit`.

### Changed

- Keys are marshalled with `Marshaler::marshalItem()` instead of `marshalJson(json_encode(...))`. String and number keys produce the same AttributeValues as before.
- The `autoload-dev` namespace is `VictorMacko\Tests\` (it was `App\Tests\`, for a `tests/` directory that did not exist).

### Deprecated

- `convertResultsToObjectArray()` maps a single Query/Scan page. Use `queryAll()` with `denormalizeItems()`, or call it once per page of your own pagination loop.

### Upgrading

- Callers of `getOneOrNullById()` that treated `null` as "not found" now get a `DynamoDbException` for real failures. That's the fix: let it become a 5xx, or catch it where a "please retry" message is better. Never fall through to defaults.
- Callers of `saveObjects()` that relied on unencodable objects being skipped must catch `UnencodableObjectsException`. Callers must also expect `UnprocessedItemsException` under sustained throttling.
- Subclasses that already declare a method named `queryAll`, `denormalizeItems`, `batchWrite`, `marshaler` or `sleepBeforeBatchRetry` with a different signature will no longer load.
