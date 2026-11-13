# DynamoDB review — dynamodb-repository

Reviewed 2026-11-13 against the shared DynamoDB KB (/Users/victor/dev/research/dynamodb-research/, cited as DDB 0x). Branch reviewed: master (working tree incl. uncommitted changes).

State of the repo at review time:
- The local `master` is 2 commits ahead of `origin/master` (`0048f8b` "Updated to SF8", `566bd52` "Removed pkg").
- Tag `v1.0.1` points at `566bd52`, which is the commit `vte-addon-risk-assessments/composer.lock` installs from GitHub. So the tag was published even though the local tracking ref doesn't show the branch as pushed.
- `composer.json` has one uncommitted change: `ramsey/uuid` is widened to `^3.9 || ^4.0`.

This is a **library**: it owns no tables. Because of that, the review is a redesign proposal. It measures the package against two parts of the KB:
- DDB 01 §5.2 and §7.5: don't use an ORM; write a thin, DynamoDB-shaped data layer.
- DDB 06 §6.6: a PHP 8 key builder plus a thin repository.

The consumer-specific findings live in `/Users/victor/dev/www/vte-addon-risk-assessments/DYNAMODB-REVIEW.md`.

## Summary

`victormacko/dynamodb-repository` v1.0.1 is a two-class base for per-table repositories.
- It wraps GetItem, PutItem, BatchWriteItem and DeleteItem, using the Symfony Serializer and `Marshaler::marshalJson`.
- Its one Composer consumer is `vte-addon-risk-assessments`.
- `microservice-holidays`, `vte-addon-hcp` and `vte-addon-wallboard` each carry a forked copy.

Its *shape* is right by DDB 01 §5.2: there is no `findAll`/`findBy`/Scan helper, and every method is one visible request. Its *behaviour* is unsafe:
- `getOneOrNullById` turns every DynamoDB error (throttling, AccessDenied, missing table, timeouts) into `null`, which reads as "not found".
- `saveObjects` silently drops `UnprocessedItems` and fails a whole chunk on duplicate keys.
- Writes can't carry a `ConditionExpression`.
- There is no ConsistentRead option.
- There is no pagination primitive, so every consumer hand-rolls one, and wallboard's fork doesn't paginate at all.

The holidays fork is the better technical base:
- constructor injection;
- one shared Marshaler;
- no swallowing;
- a paginated `queryAll()`;
- a `batchWrite()` that retries `UnprocessedItems` and throws when it gives up;
- `Aws\MockHandler` tests.

**Top 3 actions:**
1. Release **1.1.0**. It stops swallowing read errors, adds `consistent_read` and condition pass-through, and makes `saveObjects` dedupe, retry and throw (F1, F2, F4, F5).
2. Port holidays' `queryAll`/`batchWrite` and their tests into the package. Add a cursor-returning `queryPage()` and a bounded `queryAll()`. Fix the Composer constraints (F3, F6).
3. Build **2.0** around a composable `Table` helper:
   - conditional `create()`;
   - optimistic `putVersioned()`;
   - targeted `update()`;
   - `batchGet`/`batchWrite` with retries;
   - signed cursors;
   - a `Ttl` helper;
   - mapping of condition and transaction errors.

   Then move the three forks onto it.

## Current design

### Tables
None. Each consumer passes its raw `table_name` and `key` map on every call. The package has no key builder, no table-name resolution, no `Type` attribute and no PK/SK convention. That fits its consumers, which are small one-entity-per-table services (DDB 02 §3.5).

### API (`src/Repository/AbstractDynamoDbRepository.php`, `src/Repository/DynamoDbRepositoryHelper.php`)

| Method (visibility) | Lines | Request | Behaviour today | Problem |
|---|---|---|---|---|
| `setServices(DynamoDbClient, LoggerInterface, SerializerInterface)` (public, `#[Required]`) | 33-42 | – | Setter injection into untyped protected props | Any subclass built outside the DI container has null services (F7) |
| `getOneOrNullById(['table_name','key','class_name','denormalize_context'])` (public) | 44-84 | GetItem (64-69), `Key` = `marshalJson(json_encode($key))` | Eventually consistent. Returns null when there is no `Item` (71-73). **`catch (DynamoDbException) { return null; }`** (81-83) | AP-04 (F1); no `ConsistentRead` (F5) |
| `convertResultsToObjectArray(Result, $class, ['denormalize_context'])` (protected) | 86-115 | none | Maps `$result['Items']` of **one** page | No pagination primitive; `LastEvaluatedKey` is ignored (F3) |
| `saveObject($obj, ['table_name','serialize_context'])` (protected) | 117-139 | PutItem (133-138) | Serializes the whole object to JSON, then `marshalJson` | Always unconditional; full overwrite (F4) |
| `saveObjects(array, ['table_name','serialize_context'])` (protected) | 141-181 | BatchWriteItem (171-179) | `array_chunk(…, 25)` (156). An unencodable object is logged and skipped (166-168) | **`UnprocessedItems` discarded**; duplicate keys fail the chunk (F2) |
| `deleteByKey(['table_name','key'])` (protected) | 183-204 | DeleteItem (198-203) | Unconditional; no `ReturnValues` | Can't express "must exist" (F4) |
| `DynamoDbRepositoryHelper::{convertResultsToObjectArray, deleteByKey, saveObject}` | 13-29 | – | Makes the protected methods public, for composition | `saveObjects` isn't exposed; harmless |

A `new Marshaler()` is built on every call (lines 62, 97, 130, 154, 196). The package has no Query, Scan, UpdateItem, BatchGetItem, transactions or TTL support.

### Consumers and forks

| Project | How it uses the code | State (verified 2026-11-13) |
|---|---|---|
| `vte-addon-risk-assessments` | Composer `"victormacko/dynamodb-repository": "*"` (composer.json:49). The lock is v1.0.1 @ 566bd52, and the vendored copy is byte-identical to `src/` (`diff -r`) | 4 repositories extend the base. Its own `QueriesByClientIdTrait.php:21-40` paginates with `getPaginator('Query')`, which is correct |
| `microservice-holidays` | Fork, `src/Repository/AbstractDynamoDbRepository.php` (81 lines), committed | Rewritten: ctor injection (21-27); shared Marshaler (26); `queryAll()` via paginator (36-46); `batchWrite()` with `UnprocessedItems` retry, exponential backoff capped at 5 s and 8 attempts, then `RuntimeException` (57-80). Dedupe lives in the subclass (`PublicHolidayRepository.php:133-145`). MockHandler tests: `tests/Functional/PublicHolidayRepositoryTest.php:30-100` |
| `vte-addon-hcp` | Fork, `src/Repository/AbstractDynamoDbRepository.php`. **Uncommitted** changes in its working tree | Still has setter injection and `TokenStorage`/`getDomainFromUser()` (88-99), plus a `filterFn` PHP-side filter (101-127). `getOneOrNullById` now logs but **still returns null** (81-85). Adds `queryAllPages()`, a correct do/while on `LastEvaluatedKey` (188-221), and `batchWriteWithRetry()`, 6 attempts, no jitter, no dedupe, throws (229-255) |
| `vte-addon-wallboard` | Fork, `src/Repository/AbstractDynamoDbRepository.php` (139 lines) | The oldest copy. `getOneOrNullById` calls `unmarshalItem($result['Item'])` with no null check (74), so a missing item is a `TypeError`, not null. `dump($ret)` is left in `convertResultsToObjectArray` (98), which `ConfigRepository.php:33` calls after an unpaginated `scan` (23-31). Pass this to the wallboard review |

### Scorecard against DDB 01 §5.2 (ORM-shaped vs DynamoDB-shaped)

| Smell (DDB 01 §5.2) | v1.0.1 | Target |
|---|---|---|
| `findAll`/`findBy` → Scan + filter | Absent ✔ | Keep it absent |
| `save()` → unconditional PutItem | `saveObject` is exactly this ✘ | `create()` with `attribute_not_exists`; `putVersioned()`; `update()` with `attribute_exists` |
| Hidden pagination (page 1 only) | `convertResultsToObjectArray` maps a single page ✘ | `queryPage()` → `(items, cursor)`; an explicit, bounded `queryAll()` |
| Swallowing exceptions | Every GetItem error → null ✘ | Null only when there is no `Item`; condition failures become domain exceptions; everything else propagates |
| Options not passed through | No `ConsistentRead`, `ReturnValues`, `ConditionExpression` or `ReturnConsumedCapacity` ✘ | Each named on the method that needs it |
| Ad hoc marshalling | `marshalJson(json_encode(...))`, a new Marshaler per call (~) | One Marshaler; `marshalItem` for keys and values |
| Index attributes as entity fields | n/a (consumers have no GSIs) | Key building stays in the consumer's key builder |

## Findings

### Critical (correctness)

**F1 — `getOneOrNullById` turns every DynamoDB error into "not found" (AP-04)** · Correctness
- **Problem:** `src/Repository/AbstractDynamoDbRepository.php:61-83`. The `try` wraps the GetItem, and `catch (DynamoDbException $e) { return null; }` (81-83) neither logs nor rethrows.
- **Why it matters:** GetItem has no "not found" exception. A missing item is just a response with no `Item`, which line 71 already handles. So **every** exception that reaches the catch is a real failure:
  - `ProvisionedThroughputExceededException`, once the SDK's 10 legacy retries are used up;
  - `AccessDeniedException`;
  - `ResourceNotFoundException` (wrong table or stage);
  - `ValidationException` (e.g. an int key against an S attribute, which `json_encode` makes easy);
  - network errors and timeouts.

  Callers can't tell any of these from "absent". In risk-assessments that makes a saved assessment look new: the form is pre-filled with defaults and then saved over the real one. It also makes tenant settings look empty, so the high-risk notification email silently isn't sent. None of this reaches logs or alarms (DDB 01 §5.2; DDB 08 AP-04).
- **Fix (1.1.0):** remove the try/catch. Return null only when `!isset($result['Item'])`, and let the `DynamoDbException` propagate.

  ```php
  $result = $this->dynamoDbClient->getItem([
      'TableName'      => $options['table_name'],
      'Key'            => $this->marshaler()->marshalItem($options['key']),
      'ConsistentRead' => $options['consistent_read'],          // new option, default false (F5)
  ]);
  if (!isset($result['Item'])) {
      return null;                                              // the only "not found"
  }
  return $this->serializer->denormalize($this->marshaler()->unmarshalItem($result['Item']),
      $options['class_name'], null, $options['denormalize_context']);
  ```

  Ship this as a bug fix in a minor release. The only Composer consumer (risk-assessments) wants exactly this behaviour, because every caller there treats null as "absent".
- **Effort:** S. **Risk:** low. Consumers now see 5xx where they used to see silent wrong data, and that's the point.
- **Migration/infra:** none.

**F2 — `saveObjects` can lose writes (AP-03)** · Correctness (latent)
- **Problem:** `AbstractDynamoDbRepository.php:141-181` has three holes:
  1. The `batchWriteItem` result is discarded (171-179). Throttled entries come back in `UnprocessedItems` with HTTP 200 and are lost.
  2. There is no key de-duplication inside a 25-item chunk (156-170). Two objects with the same key make DynamoDB reject the **whole** chunk with a `ValidationException`.
  3. A `NotEncodableValueException` is logged and that object skipped (166-168). The caller gets no signal that some objects weren't written.
- **Why it matters:** this is silent data loss under exactly the conditions batch writes meet: bulk imports on small or provisioned tables (DDB 01 §4.1, §7.4). No package consumer calls it today; risk-assessments doesn't use it. But `vte-addon-hcp` calls the same code in its fork (`ItemRepository.php:163`, `MemberRepository.php:170`), and its uncommitted working tree has just patched the retry part locally.
- **Fix (1.1.0):** do three things:
  - de-duplicate by key, with the last request per key winning;
  - chunk to 25;
  - retry `UnprocessedItems` with capped exponential backoff plus jitter, then throw an exception that carries the leftovers.

  Port holidays' loop (`microservice-holidays/src/Repository/AbstractDynamoDbRepository.php:57-80`) and its three tests (`tests/Functional/PublicHolidayRepositoryTest.php:61-100`). Add jitter and the key dedupe that holidays does in its subclass. The unencodable-object behaviour becomes an option: `'on_unencodable' => 'skip'` (the 1.x default, but now returned or thrown in aggregate after the batch) or `'throw'` (the 2.0 default). See `Table::batchWrite()` in the sketch below.
- **Effort:** S. **Risk:** low.
- **Migration/infra:** none.

### High

**F3 — No pagination primitive; one-page mapping invites truncation (AP-01, AP-38)** · Correctness / design
- **Problem:** `convertResultsToObjectArray` (`:86-115`) maps only the `Items` of the one `Result` it's given. The package offers no Query helper, so every consumer writes its own pagination:
  - risk-assessments, correctly: `QueriesByClientIdTrait.php:21-40`;
  - hcp, correctly: `queryAllPages`, uncommitted;
  - holidays, correctly: `queryAll`;
  - wallboard, **not at all**: `ConfigRepository.php:23-33`.
- **Why it matters:** each Query or Scan page stops at 1 MB, measured before filters. The natural way to use this API (call `query()`, pass the Result to `convertResultsToObjectArray`) silently returns page 1 only (DDB 01 §4.4, §5.3.7; DDB 09 §2.7: "a non-empty `LastEvaluatedKey` marks a page boundary, not a promise of more matches").
- **Fix:**
  - **1.1.0:** add `protected function queryAll(array $params, int $maxItems = 1000): array`. It uses `getPaginator('Query')`, unmarshals, and **throws** past `$maxItems`, so a drain can be neither silently truncated nor unbounded. It keeps the same name and argument as holidays' fork, so holidays can migrate unchanged.
  - **1.1.0:** add `protected function queryPage(array $params, int $limit, ?string $cursor): Page`. It returns items plus an opaque, signed cursor (DDB 01 §7.3; DDB 05 §6.3).
  - Mark `convertResultsToObjectArray` `@deprecated`.
- **Effort:** S–M (with tests). **Risk:** low.

**F4 — Writes cannot carry conditions; no targeted update (AP-05, AP-06, AP-21)** · Correctness (pushed onto consumers)
- **Problem:**
  - `saveObject` (`:117-139`) always sends an unconditional whole-item PutItem.
  - `deleteByKey` (`:183-204`) is unconditional.
  - There is no UpdateItem helper.
- **Why it matters:** a consumer can't express "create only" (`attribute_not_exists`), "update only if it exists" (`attribute_exists`), or "only if nobody changed it since I read it" (a version check). Every read-modify-write therefore becomes last-writer-wins. Risk-assessments' assessment save is one: `PerformController.php:70-91` (DDB 01 §6.4 patterns 1, 2, 7; §7.4).
- **Fix:**
  - **1.1.0 (BC):** add pass-through options to `saveObject` and `deleteByKey`: `condition_expression`, `expression_attribute_names`, `expression_attribute_values` (native PHP values, marshalled by the library). Map `ConditionalCheckFailedException` to `VictorMacko\Repository\Exception\ConditionFailed` (previous = the SDK exception). This is thrown only when the caller supplied a condition, so existing calls behave exactly as before.
  - **2.0:** `Table::create()`, `putVersioned()`, `update()` and `delete(mustExist:)`. See the sketch.
- **Effort:** S (1.1) / M (2.0). **Risk:** low.

### Medium

**F5 — No `ConsistentRead` option (AP-13)** · Correctness (minor)
- **Problem:** `getOneOrNullById` (`:64-69`) always does an eventually consistent read.
- **Why it matters:** consumers redirect after a save and then re-read the item (risk-assessments: `PerformController.php:120` → `:70`). An eventual read can return the pre-save state, and that breaks any version check built on F4. A strong read costs 1 RCU instead of 0.5 per 4 KB (DDB 01 §5.3.1; DDB 07 §4.2).
- **Fix:** add a `consistent_read` option, default `false`, in 1.1.0, and a `consistent` argument on `Table::get()`, `Query::consistent()` and `batchGet()` in 2.0. Never allow it on a GSI query (ValidationException).
- **Effort:** S.

**F6 — Dependency constraints and missing tests** · Hygiene
- **Problem:** `composer.json:12-19, 37-42`:
  - **`"symfony/options-resolver": "^5.0||^6.0||7.0||8.0"`** (`:17`). Composer reads the bare `7.0` and `8.0` as exact versions, 7.0.0 and 8.0.0. Risk-assessments' lock shows the effect: `symfony/options-resolver v8.0.0` next to `symfony/framework-bundle v8.1.11`. The package is pinning a Symfony component below its patch releases.
  - It hard-requires a Symfony *bundle* and *packs*: `aws/aws-sdk-php-symfony` (`:14`), `symfony/monolog-bundle` (`:16`), `symfony/serializer-pack` (`:18`). A library needs only `aws/aws-sdk-php`, `psr/log` and `symfony/serializer`.
  - `ramsey/uuid` (`:15`) isn't used in `src/`.
  - `autoload-dev` points at a `tests/` directory that doesn't exist (`:37-41`).
  - The consumer requires `"*"`, so a future 2.0 would arrive on any `composer update`.
- **Fix:**
  - **1.0.2:**
    - `"php": ">=8.1"`;
    - `"aws/aws-sdk-php": "^3.300"`;
    - `"psr/log": "^1.0 || ^2.0 || ^3.0"`;
    - `"symfony/serializer": "^5.4 || ^6.4 || ^7.0 || ^8.0"`;
    - `"symfony/options-resolver": "^5.4 || ^6.4 || ^7.0 || ^8.0"`, dropped in 2.0;
    - drop `ramsey/uuid`, `monolog-bundle`, `serializer-pack` and `aws-sdk-php-symfony`;
    - add `phpunit/phpunit` to dev and a `tests/` dir with `VictorMacko\Tests\` autoload.
  - Push `master`.
  - Consumers pin `^1.0` now (the risk-assessments review, F11).
- **Effort:** S. **Risk:** low. Check that risk-assessments still resolves; it requires `aws/aws-sdk-php-symfony` itself.

### Low

**F7 — Setter injection, a Marshaler per call, JSON round-trip for keys** · Hygiene (AP-45-adjacent)
- **Problem:**
  - Untyped protected props filled by a `#[Required]` setter (`:19-42`).
  - A `new Marshaler()` in every method (`:62, :97, :130, :154, :196`).
  - Keys marshalled with `marshalJson(json_encode($key))` (`:67, :201`). `json_encode` returns `false` on invalid UTF-8, which then fails obscurely inside `marshalJson`.
- **Why it matters:** this is not a cost problem, but a subclass built outside the container silently has null services, and the key typing is indirect.
- **Fix:**
  - **1.1:** a lazily created shared Marshaler, `protected function marshaler(): Marshaler`, and `marshalItem($key)` for keys.
  - **2.0:** constructor injection with readonly properties, as in holidays (`:21-27`).
- **Effort:** S.

**F8 — Tenant lookup and PHP-side filtering crept into the forks** · Design (AP-38, AP-17)
- **Problem:** `vte-addon-hcp` and `vte-addon-wallboard` put `TokenStorageInterface`/`getDomainFromUser()` in the base class (hcp `:27-51, :88-99`; wallboard `:25-49`). hcp adds a `filterFn` that filters rows after reading them (hcp `:101-127`).
- **Why it matters:** if those come back into the shared package, it starts resolving tenants and filtering in PHP, which is the ORM drift DDB 01 §5.2 warns about.
- **Fix:** keep them out of the package. The tenant id is an explicit argument (or an app-level `TenantAware` trait), and filtering belongs in key or index design, which is for the hcp review to decide.
- **Effort:** S (at fork-migration time).

## Proposed target design (the redesign)

### Principles (DDB 01 §5.2, §7.5; DDB 06 §6.2, §6.6)
1. Each public method is **one visible DynamoDB request**. The only additions are the retries of unprocessed batch items.
2. The package owns **request mechanics**:
   - marshalling;
   - pagination and cursors;
   - batch chunking, dedupe and retry;
   - condition plumbing and mapping of condition and transaction errors;
   - TTL arithmetic.

   Consumers own **access patterns and key formats**, with one key builder per entity, and name their repository methods after patterns (`findByEvent`, `listCriteriaForTenant`).
3. Errors are never swallowed:
   - "Not found" is "no `Item` in the response".
   - Condition and transaction failures become domain exceptions.
   - Everything else propagates as the SDK's `DynamoDbException`, so callers can still `getAwsErrorCode()` it.
4. Table names come in through the constructor (from env), never as literals.

### What the base should provide
| Capability | API (2.0) | 1.1 equivalent | KB |
|---|---|---|---|
| GetItem, null only when absent; optional strong read; optional TTL guard | `Table::get(array $key, bool $consistent = false, ?string $ttlAttribute = null): ?array` | `getOneOrNullById([... 'consistent_read' => true])` | DDB 01 §5.3.1, §7.3 |
| One Query page plus an opaque, signed, partition-bound cursor | `Table::queryPage(Query $q, int $limit, ?string $cursor): Page` | `queryPage(array $params, int $limit, ?string $cursor)` | DDB 01 §7.3; DDB 05 §6.3 |
| Bounded drain of a small collection (throws when exceeded) | `Table::queryAll(Query $q, int $maxItems = 1000): array` | `queryAll(array $params, int $maxItems = 1000)` | DDB 01 §7.3 |
| Conditional create | `Table::create(array $item)` → `ItemAlreadyExists` | `saveObject([... 'condition_expression' => 'attribute_not_exists(#pk)'])` | DDB 01 §7.4 |
| Optimistic locking | `Table::putVersioned(array $item, ?int $expected, string $attr = 'version'): int` → `ConcurrentModification` | condition pass-through | DDB 01 §6.4 #7 |
| Targeted update, no accidental upsert | `Table::update(array $key, set:, remove:, add:, condition:, mustExist: true): array` (ALL_NEW) → `ItemNotFound` | – | DDB 01 §6.5, §7.4 |
| Delete, optionally must-exist, returns old item | `Table::delete(array $key, bool $mustExist = false): ?array` | `deleteByKey([... condition])` | DDB 01 §6.4 #2 |
| Batch get: dedupe, 100 per call, `UnprocessedKeys` retry | `Table::batchGet(array $keys, bool $consistent = false): array` | `batchGet(string $table, array $keys)` | DDB 01 §4.1; DDB 08 AP-03 |
| Batch write: dedupe, 25 per call, `UnprocessedItems` retry with jitter, throw leftovers | `Table::batchWrite(array $puts = [], array $deleteKeys = []): void` | `saveObjects` fixed + `batchWrite(string $table, array $requests)` | DDB 01 §7.4; DDB 06 §6.6 |
| Error mapping | `Errors::map()`; `Errors::isRetryable()` | `ConditionFailed` only | DDB 01 §5.2, §7.4 |
| TTL helper | `Ttl::in()`, `Ttl::at()`, `Ttl::isLive()`, `Ttl::liveFilter()` | – | DDB 01 §3.2; DDB 09 §2.6 |
| Object mapping (optional) | `ItemMapper` interface + `SerializerItemMapper` adapter | the current Serializer path | – |
| Cost tracing (optional) | `Table(... traceCapacity: true)` adds `ReturnConsumedCapacity => 'TOTAL'` and logs it at debug | – | DDB 08 §1 step 4 |
| Transactions (when a consumer first needs them) | `Table::transactWrite(array $items)` → `TransactionCancelled::failedIndexes()` | – | DDB 06 §6.4, §6.6 |

### What it should NOT provide
- **`findAll()` via Scan, `findBy([...])` or criteria builders.** These can only be served by Scan + FilterExpression, so their cost grows with the table (DDB 01 §5.2; AP-15/AP-16). A consumer that needs "all X" models it as a Query on a key or GSI.
- **A generic `save()` that is always an unconditional PutItem.** The method name must say what it does: `create`, `putVersioned`, `upsert` for idempotent deterministic keys only, or `update`.
- **Silent draining.** No "get everything" method without a bound, and no method that returns only page 1.
- **Tenant resolution from the security token, or PHP-side row filters** (see F8).
- **Generic `PK`/`SK` or single-table conventions.** The consumers are small multi-table services, and DDB 02 §3.5 says they're right to stay that way. `Table` takes the real key attribute names (`clientId`/`id`, `id`).
- **Its own retry loop around throttling exceptions.** The SDK already retries those. Configure `retries` on the client (DDB 09 §7.1). Only batch leftovers, which aren't exceptions, get a package-level retry.
- **Hard dependencies on Symfony bundles.** The Serializer becomes an optional adapter.

### PHP 8 API sketch (2.0; PHP ≥ 8.1, aws/aws-sdk-php v3)

```php
<?php
declare(strict_types=1);

namespace VictorMacko\Repository;

use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDb\Exception\DynamoDbException;
use Aws\DynamoDb\Marshaler;
use Aws\Result;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use VictorMacko\Repository\Exception\{ConcurrentModification, ConditionFailed, ItemAlreadyExists, ItemNotFound,
    ResultTooLarge, UnprocessedItemsException};

/** One instance per table, injected into a consumer's repository. Every method = one visible request. */
final class Table
{
    public function __construct(
        private readonly DynamoDbClient $db,
        public readonly string $name,               // from env (e.g. %env(DDB_TABLE_EVENTS)%), never a literal
        private readonly string $pk,                // partition-key attribute, e.g. 'clientId'
        private readonly ?string $sk = null,        // sort-key attribute, e.g. 'id' (null for simple keys)
        private readonly Marshaler $m = new Marshaler(),
        private readonly LoggerInterface $logger = new NullLogger(),
        private readonly ?CursorCodec $cursors = null,   // required only for queryPage()
    ) {}

    /** GetItem. Null ONLY when there is no item (or it has expired by $ttlAttribute). Errors propagate. */
    public function get(array $key, bool $consistent = false, ?string $ttlAttribute = null): ?array
    {
        $r = $this->call('getItem', [
            'TableName' => $this->name,
            'Key' => $this->m->marshalItem($this->onlyKey($key)),
            'ConsistentRead' => $consistent,
        ]);
        if (!isset($r['Item'])) {
            return null;
        }
        $item = $this->m->unmarshalItem($r['Item']);
        return ($ttlAttribute === null || Ttl::isLive($item, $ttlAttribute)) ? $item : null;
    }

    /** One page. Limit counts items EVALUATED (DDB 01 §5.3.6). Cursor is signed and bound to the partition. */
    public function queryPage(Query $q, int $limit = 50, ?string $cursor = null): Page
    {
        $codec = $this->cursors ?? throw new \LogicException('queryPage() needs a CursorCodec');
        $params = $q->toParams($this->name, $this->m) + ['Limit' => $limit];
        if ($cursor !== null) {
            $params['ExclusiveStartKey'] = $codec->decode($cursor, $q->partitionValue());
        }
        $r = $this->call('query', $params);
        return new Page(
            array_map(fn (array $raw): array => $this->m->unmarshalItem($raw), $r['Items'] ?? []),
            isset($r['LastEvaluatedKey']) ? $codec->encode($r['LastEvaluatedKey'], $q->partitionValue()) : null,
        );
    }

    /** Deliberate drain of a SMALL collection. Never truncates silently, never runs away. */
    public function queryAll(Query $q, int $maxItems = 1_000): array
    {
        $out = [];
        try {
            foreach ($this->db->getPaginator('Query', $q->toParams($this->name, $this->m)) as $page) {
                foreach ($page['Items'] ?? [] as $raw) {
                    if (count($out) === $maxItems) {
                        throw new ResultTooLarge("{$this->name}: more than $maxItems items for {$q->partitionValue()}");
                    }
                    $out[] = $this->m->unmarshalItem($raw);
                }
            }
        } catch (DynamoDbException $e) {
            throw Errors::map($e, $this->name);
        }
        return $out;
    }

    /** Create-only PutItem (DDB 01 §7.4 pattern 1). */
    public function create(array $item): void
    {
        try {
            $this->call('putItem', [
                'TableName' => $this->name,
                'Item' => $this->m->marshalItem($item),
                'ConditionExpression' => 'attribute_not_exists(#pk)',
                'ExpressionAttributeNames' => ['#pk' => $this->pk],
            ]);
        } catch (ConditionFailed $e) {
            throw new ItemAlreadyExists($this->name, $this->onlyKey($item), $e);
        }
    }

    /**
     * Optimistic lock on a whole-item Put. $expected = the version the user saw; null = "I started from nothing".
     * Items written before versioning have no attribute and count as version 0 (lazy default, DDB 05 §2.1).
     */
    public function putVersioned(array $item, ?int $expected, string $attr = 'version'): int
    {
        $next = ($expected ?? 0) + 1;
        $cond = match (true) {
            $expected === null => Expr::of('attribute_not_exists(#pk)', ['#pk' => $this->pk]),
            $expected === 0    => Expr::of('attribute_exists(#pk) AND (attribute_not_exists(#v) OR #v = :exp)',
                                           ['#pk' => $this->pk, '#v' => $attr], [':exp' => 0]),
            default            => Expr::of('#v = :exp', ['#v' => $attr], [':exp' => $expected]),
        };
        try {
            $this->call('putItem', ['TableName' => $this->name, 'Item' => $this->m->marshalItem([$attr => $next] + $item),
                'ReturnValuesOnConditionCheckFailure' => 'ALL_OLD'] + $cond->toParams($this->m));
        } catch (ConditionFailed $e) {
            throw new ConcurrentModification($this->name, $this->onlyKey($item), $expected, $e->item, $e);
        }
        return $next;
    }

    /** Targeted UpdateItem: only the named attributes change; no accidental upsert (DDB 01 §6.5). Returns ALL_NEW. */
    public function update(array $key, array $set = [], array $remove = [], array $add = [],
                           ?Expr $condition = null, bool $mustExist = true): array
    {
        $exists = $mustExist ? Expr::of('attribute_exists(#pk)', ['#pk' => $this->pk]) : null;
        $u = UpdateExpression::build($set, $remove, $add);     // generates #a0/:v0 placeholders; never interpolates
        try {
            $r = $this->call('updateItem', [
                'TableName' => $this->name,
                'Key' => $this->m->marshalItem($this->onlyKey($key)),
                'ReturnValues' => 'ALL_NEW',
                'ReturnValuesOnConditionCheckFailure' => 'ALL_OLD',   // ⚠ since 2023 (DDB 09 §2.4)
            ] + $u->toParams($this->m, Expr::all($exists, $condition)));
        } catch (ConditionFailed $e) {
            throw ($mustExist && $e->item === null) ? new ItemNotFound($this->name, $this->onlyKey($key), $e) : $e;
        }
        return $this->m->unmarshalItem($r['Attributes']);
    }

    /** Deletes; with $mustExist a missing item is ItemNotFound instead of a silent success. Returns the old item. */
    public function delete(array $key, bool $mustExist = false): ?array { /* DeleteItem, ReturnValues ALL_OLD */ }

    /** BatchGetItem: dedupe keys, 100 per call, retry UnprocessedKeys (same backoff as batchWrite). */
    public function batchGet(array $keys, bool $consistent = false): array { /* ... */ }

    /** BatchWriteItem: dedupe, 25 per call, retry UnprocessedItems with capped backoff + full jitter, then throw. */
    public function batchWrite(array $puts = [], array $deleteKeys = [], int $maxAttempts = 6, int $baseDelayMs = 50): void
    {
        $requests = [];
        foreach ($puts as $item) {                     // one request per key: DynamoDB rejects duplicates in a batch
            $requests[$this->keyId($item)] = ['PutRequest' => ['Item' => $this->m->marshalItem($item)]];
        }
        foreach ($deleteKeys as $key) {
            $id = $this->keyId($key);
            if (isset($requests[$id])) {
                throw new \InvalidArgumentException("Key $id is both put and deleted in one batch");
            }
            $requests[$id] = ['DeleteRequest' => ['Key' => $this->m->marshalItem($this->onlyKey($key))]];
        }
        foreach (array_chunk(array_values($requests), 25) as $chunk) {
            $pending = [$this->name => $chunk];
            for ($attempt = 1; ; $attempt++) {
                $pending = $this->call('batchWriteItem', ['RequestItems' => $pending])['UnprocessedItems'] ?? [];
                if ($pending === []) {
                    break;
                }
                if ($attempt >= $maxAttempts) {
                    throw new UnprocessedItemsException($this->name, $pending[$this->name] ?? []);
                }
                $this->logger->warning('DynamoDB returned unprocessed items, retrying',
                    ['table' => $this->name, 'remaining' => count($pending[$this->name] ?? []), 'attempt' => $attempt]);
                usleep(random_int(0, min(5_000, $baseDelayMs * 2 ** $attempt)) * 1_000);
            }
        }
    }

    private function call(string $op, array $params): Result
    {
        try {
            return $this->db->{$op}($params);
        } catch (DynamoDbException $e) {
            throw Errors::map($e, $this->name);
        }
    }

    private function onlyKey(array $item): array
    {
        $key = [];
        foreach (array_filter([$this->pk, $this->sk]) as $attr) {
            $key[$attr] = $item[$attr] ?? throw new \InvalidArgumentException("{$this->name}: missing key attribute $attr");
        }
        return $key;
    }

    private function keyId(array $item): string
    {
        return json_encode($this->onlyKey($item), JSON_THROW_ON_ERROR);
    }
}

final class Errors
{
    /** Condition/transaction failures become domain exceptions; everything else is rethrown unchanged. */
    public static function map(DynamoDbException $e, string $table): \Throwable
    {
        return match ($e->getAwsErrorCode()) {
            'ConditionalCheckFailedException' => new ConditionFailed($table, self::item($e), $e),
            'TransactionCanceledException'    => new Exception\TransactionCancelled($table, $e->get('CancellationReasons') ?? [], $e),
            default                           => $e,   // throttling, AccessDenied, ResourceNotFound, Validation, network
        };
    }

    /** For callers that want to show "try again" rather than an error page. The SDK has already retried these. */
    public static function isRetryable(\Throwable $e): bool
    {
        return $e instanceof DynamoDbException && in_array($e->getAwsErrorCode(),
            ['ProvisionedThroughputExceededException', 'ThrottlingException', 'RequestLimitExceeded',
             'TransactionConflictException', 'InternalServerError'], true);
    }

    private static function item(DynamoDbException $e): ?array
    {
        $raw = $e->get('Item');                         // present with ReturnValuesOnConditionCheckFailure; verify on your SDK
        return is_array($raw) ? (new Marshaler())->unmarshalItem($raw) : null;
    }
}

/** Key condition + options for one Query; partitionValue() binds cursors to the partition they came from. */
final class Query
{
    private function __construct(
        private string $condition, private array $names, private array $values, private string $partition,
        private ?string $index = null, private bool $forward = true, private bool $consistent = false,
    ) {}

    public static function partition(string $attr, string $value): self
    {
        return new self('#pk = :pk', ['#pk' => $attr], [':pk' => $value], $value);
    }
    public function skBeginsWith(string $attr, string $prefix): self { /* AND begins_with(#sk, :skp) */ }
    public function skBetween(string $attr, string $from, string $to): self { /* AND #sk BETWEEN :a AND :b */ }
    public function onIndex(string $index): self { /* consistent() on an index => LogicException (GSIs are eventual) */ }
    public function newestFirst(): self { $c = clone $this; $c->forward = false; return $c; }
    public function consistent(): self { $c = clone $this; $c->consistent = true; return $c; }
    public function partitionValue(): string { return $this->partition; }

    public function toParams(string $table, Marshaler $m): array
    {
        return array_filter([
            'TableName' => $table, 'IndexName' => $this->index,
            'KeyConditionExpression' => $this->condition,
            'ExpressionAttributeNames' => $this->names,
            'ExpressionAttributeValues' => $m->marshalItem($this->values),
            'ScanIndexForward' => $this->forward ? null : false,
            'ConsistentRead' => $this->consistent ?: null,
        ], static fn ($v) => $v !== null);
    }
}

/** Expression fragment with its own placeholders. Never sends empty name/value maps (ValidationException). */
final class Expr
{
    private function __construct(public readonly string $expression, public readonly array $names, public readonly array $values) {}
    public static function of(string $expression, array $names = [], array $values = []): self { return new self($expression, $names, $values); }
    public static function all(?self ...$parts): ?self { /* AND-join the non-null parts, merge maps, reject placeholder clashes */ }
    public function toParams(Marshaler $m, string $as = 'ConditionExpression'): array
    {
        return [$as => $this->expression]
            + ($this->names === [] ? [] : ['ExpressionAttributeNames' => $this->names])
            + ($this->values === [] ? [] : ['ExpressionAttributeValues' => $m->marshalItem($this->values)]);
    }
}

/** Opaque, HMAC-signed cursor, bound to one partition so a client can't page into another tenant. */
final class CursorCodec
{
    public function __construct(private readonly string $secret) {}

    public function encode(array $lastEvaluatedKey, string $partition): string
    {
        $json = json_encode(['k' => $lastEvaluatedKey, 'p' => $partition], JSON_THROW_ON_ERROR);  // B keys: base64 first
        return rtrim(strtr(base64_encode($json), '+/', '-_'), '=') . '.' . hash_hmac('sha256', $json, $this->secret);
    }

    public function decode(string $cursor, string $partition): array
    {
        [$b64, $mac] = explode('.', $cursor, 2) + [1 => ''];
        $json = base64_decode(strtr($b64, '-_', '+/'), true);
        if ($json === false || !hash_equals(hash_hmac('sha256', $json, $this->secret), $mac)) {
            throw new \InvalidArgumentException('Invalid cursor');
        }
        $d = json_decode($json, true, flags: JSON_THROW_ON_ERROR);
        if (($d['p'] ?? null) !== $partition) {
            throw new \InvalidArgumentException('Cursor belongs to another partition');
        }
        return $d['k'];
    }
}

/** TTL must be a Number of epoch SECONDS; deletion lags by up to days, so always check on read (DDB 09 §2.6). */
final class Ttl
{
    public static function in(int $seconds, ?int $now = null): int { return ($now ?? time()) + $seconds; }
    public static function at(\DateTimeInterface $t): int { return $t->getTimestamp(); }   // not format('U'): that's a string
    public static function isLive(array $item, string $attr, ?int $now = null): bool
    {
        return !isset($item[$attr]) || (int) $item[$attr] > ($now ?? time());
    }
    public static function liveFilter(string $attr, ?int $now = null): Expr
    {
        return Expr::of('(attribute_not_exists(#ttl) OR #ttl > :now)', ['#ttl' => $attr], [':now' => $now ?? time()]);
    }
}

final class Page
{
    /** @param list<array<string,mixed>> $items */
    public function __construct(public readonly array $items, public readonly ?string $cursor) {}
}

interface ItemMapper
{
    public function toItem(object $entity): array;
    public function fromItem(array $item): object;
}
// SerializerItemMapper implements ItemMapper with Symfony (de)normalizers — the 1.x behaviour, now optional.

// Exception\ConditionFailed (readonly ?array $item) is the parent of ItemAlreadyExists, ItemNotFound and
// ConcurrentModification. All are \RuntimeException subclasses carrying the SDK exception as previous.
// UnprocessedItemsException (readonly array $remaining), ResultTooLarge, TransactionCancelled (failedIndexes()).
```

**What a consumer repository looks like on 2.0** (risk-assessments' events table):

```php
final class EventRiskKeys                       // the only place this key format lives
{
    public static function key(string $clientId, int $eventId): array
    {
        return ['clientId' => $clientId, 'id' => $eventId . '-' . $clientId];   // existing format, kept (see that review)
    }
}

final class EventRiskRepository
{
    public function __construct(private readonly Table $events, private readonly ItemMapper $mapper) {}

    /** Access pattern: the assessment for one event. Strong read: the page is shown right after a save. */
    public function findByEvent(string $clientId, int $eventId): ?EventRisk
    {
        $item = $this->events->get(EventRiskKeys::key($clientId, $eventId), consistent: true);
        return $item === null ? null : $this->mapper->fromItem($item);
    }

    /** Access pattern: save what the user submitted; conflict if someone saved in between. */
    public function save(EventRisk $r, ?int $versionSeen): int
    {
        return $this->events->putVersioned($this->mapper->toItem($r), $versionSeen);  // ConcurrentModification
    }
}
```

### Backwards-compatible migration path

| Release | Contents | BC |
|---|---|---|
| **1.0.2** | Composer constraints fixed (F6); `tests/` with an `Aws\MockHandler` harness; push `master` | Fully compatible |
| **1.1.0** | <ul><li>`getOneOrNullById` stops swallowing and gains `consistent_read` (F1, F5).</li><li>`saveObject`/`deleteByKey` accept `condition_expression` + `expression_attribute_names` + `expression_attribute_values`, with `ConditionFailed` on failure (F4).</li><li>`saveObjects` dedupes, retries and throws `UnprocessedItemsException` (F2).</li><li>New protected `queryAll(array $params, int $maxItems = 1000)`, `queryPage(...)`, `batchWrite(string $table, array $requests)`, `batchGet(...)` (F3).</li><li>A shared Marshaler (F7).</li><li>`convertResultsToObjectArray` is `@deprecated`.</li></ul> | Signatures unchanged; new options are optional. The only behaviour change is F1, a bug fix: errors that used to be silently null now propagate. Changelog entry required |
| **2.0.0** | <ul><li>`Table`, `Query`, `Expr`, `CursorCodec`, `Ttl`, `Page`, `ItemMapper` + `SerializerItemMapper`, and the exception hierarchy.</li><li>`AbstractDynamoDbRepository` becomes a deprecated thin adapter over `Table` (constructor injection), kept for one major version.</li><li>`DynamoDbRepositoryHelper` removed.</li><li>No Symfony bundle dependencies.</li></ul> | Breaking: options-array API deprecated, setter injection removed |

Consumer steps (risk-assessments):
1. Pin `^1.0` now.
2. Go to `^1.1` and adopt `consistent_read` and the conditions (its F1, F3, F5).
3. Go to `^2.0` when the repositories are next touched. Each repository then takes a `Table` built in `services.yaml` from `%env(DDB_TABLE_*)%`, and `QueriesByClientIdTrait` is replaced by `Table::queryAll(Query::partition('clientId', $tenant), 500)`.

### Reconciling the forks

| | v1.0.1 package | holidays fork | hcp fork (uncommitted working tree) | wallboard fork |
|---|---|---|---|---|
| Injection | `#[Required]` setter | **ctor, readonly** (21-27) | setter + TokenStorage | setter (`@required` docblock) |
| Marshaler | new per call | **shared** (26) | new per call | new per call |
| GetItem errors | swallowed → null | n/a (subclass calls the SDK; no catch) | logged, then null (81-85) | swallowed; missing item → TypeError (74) |
| Query pagination | none | **`queryAll` via paginator** (36-46), unbounded | `queryAllPages` do/while (188-221) | none; `scan` page 1 (ConfigRepository 23-33) |
| Batch write retry | dropped | **8 attempts, capped backoff, throws** (57-80), no jitter | 6 attempts, throws (229-255), no jitter | none |
| Dedupe in batch | no | in subclass (PublicHolidayRepository 133-145) | no | n/a |
| Conditions | no | no | no | no |
| Tests | none | **MockHandler** (PublicHolidayRepositoryTest 30-100) | – | – |

**Verdict:** holidays is the better base for the mechanics. The package contributes only the Serializer mapping and the options-array API, and keeps them for BC. The plan:
1. **Port holidays' `queryAll`/`batchWrite` and tests into the package (1.1.0)**, adding the bound, jitter and dedupe.
2. Holidays then requires `^1.1` (or `^2.0`) and deletes `src/Repository/AbstractDynamoDbRepository.php`. Its subclass already uses the same method names and `$this->marshaler`. In 1.1, `$this->marshaler` stays reachable through `marshaler()`. Holidays doesn't need the Serializer, so 2.0's optional mapper suits it.
3. hcp commits its working-tree fixes first, then moves to `^1.1`:
   - `queryAllPages()` maps to `queryAll()`;
   - `batchWriteWithRetry()` maps to `batchWrite()`;
   - `getDomainFromUser()` moves into an app trait;
   - `filterFn` goes away once hcp's own review settles its key design.
4. Wallboard does the same after its own review fixes the `dump()` (98) and the `unmarshalItem(null)` TypeError (74).
5. Once nobody copies the file, delete the "that class will shortly be migrated here" docblock (`DynamoDbRepositoryHelper.php:9`).

## Implementation plan

Each step is one PR. All tests use `Aws\MockHandler` with a client built with `'retries' => 0`, so throttling tests don't wait on SDK backoff. They need no network.

1. **1.0.2: dependency and test scaffold (S).** Apply the `composer.json` changes in F6. Add `phpunit.xml.dist`, `tests/` with `VictorMacko\Tests\` autoload, and a `MockHandler` + command-recording helper copied from holidays' test. Commit the pending `ramsey/uuid` change or drop the dependency; tag the release and push `master`. *Test:* `composer validate`; a smoke test that `getOneOrNullById` returns null for `new Result([])`.
2. **1.1.0: error and read fixes (S).** F1 and F5, plus the F7 shared Marshaler.
   *Tests:*
   - `ProvisionedThroughputExceededException` from MockHandler → exception propagates;
   - `Result([])` → null;
   - `consistent_read` → the command has `ConsistentRead: true`.
3. **1.1.0: writes (S–M).** F4 condition pass-through and `ConditionFailed`; F2 `saveObjects` dedupe, retry and throw; protected `batchWrite()`/`batchGet()` ported from holidays.
   *Tests* (holidays' three, plus):
   - 26 items → 2 calls of 25 + 1;
   - a duplicate key is collapsed;
   - `UnprocessedKeys` is retried;
   - a CCF with a condition → `ConditionFailed`.
4. **1.1.0: reads (S).** F3: `queryAll()` bounded, `queryPage()` with `CursorCodec`.
   *Tests:*
   - follows `LastEvaluatedKey` and stops only when it's absent, including an empty middle page;
   - throws past `maxItems`;
   - cursor round trip;
   - tampered or other-partition cursor rejected.

   Then tag 1.1.0, and bump risk-assessments as described in its review.
5. **2.0.0: `Table` API (M–L).** The sketch above, `SerializerItemMapper`, and the adapter `AbstractDynamoDbRepository`.
   *Tests:* each `Table` method's request shape, asserting the recorded command params, and the exception mapping, including `putVersioned` for `null`/`0`/`n`.
6. **Fork migrations (one PR per project, S each):** holidays first (closest), then hcp, then wallboard. Each runs its own existing test suite.

## Not worth changing

- **No Scan, `findAll` or `findBy` helpers.** That's correct by DDB 01 §5.2. Keep it that way. Wallboard's Scan is its own decision.
- **Symfony Serializer as the object mapper.** It suits these small, flat entities and consumers rely on it. Keep it as an optional `ItemMapper` adapter instead of removing it.
- **One table per entity, descriptive key names, no `Type` attribute.** The consumers are small services that never read different entities together (DDB 02 §3.5). The package must not impose PK/SK or single-table conventions.
- **No transactions yet.** No consumer uses them. Add `transactWrite()` with `CancellationReasons` mapping when the first one needs it (DDB 06 §6.4). IAM then needs the per-item actions (DDB 07 checklist).
- **Own batch loop instead of `Aws\DynamoDb\WriteRequestBatch`.** Both work. The holidays loop is explicit and already tested. `WriteRequestBatch` needs an `error` callback wired, or its failures are silent too (DDB 06 §6.6).
- **The CPU cost of a new Marshaler per call.** It's negligible. Fold it into the F7 tidy-up; it's not a reason to release on its own.
- **`DynamoDbRepositoryHelper`.** It's a harmless composition wrapper in 1.x. Drop it in 2.0, where `Table` is itself composable.
