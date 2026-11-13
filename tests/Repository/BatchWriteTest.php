<?php

declare(strict_types=1);

namespace VictorMacko\Tests\Repository;

use Aws\DynamoDb\Exception\DynamoDbException;
use Aws\Result;
use RuntimeException;
use Symfony\Component\Serializer\Exception\NotEncodableValueException;
use VictorMacko\Repository\Exception\UnencodableObjectsException;
use VictorMacko\Repository\Exception\UnprocessedItemsException;
use VictorMacko\Tests\Fixtures\Thing;

/**
 * F2 (AP-03): saveObjects()/batchWrite() chunk to 25, collapse duplicate keys, retry UnprocessedItems with backoff
 * and jitter, and never drop anything silently.
 */
final class BatchWriteTest extends MockDynamoDbTestCase
{
    public function testWritesInBatchesOf25(): void
    {
        $this->respond(new Result([]), new Result([]));

        $this->repository->saveObjects($this->things(26), ['table_name' => 'things']);

        self::assertCount(2, $this->commands);
        self::assertSame('BatchWriteItem', $this->commands[0]->getName());
        self::assertCount(25, $this->commands[0]['RequestItems']['things']);
        self::assertCount(1, $this->commands[1]['RequestItems']['things']);
        self::assertSame(
            ['clientId' => 'c1', 'id' => 'id-0', 'name' => 'Thing 0', 'score' => 1.5],
            self::unmarshal($this->commands[0]['RequestItems']['things'][0]['PutRequest']['Item'])
        );
    }

    public function testNothingToSaveSendsNothing(): void
    {
        $this->repository->saveObjects([], ['table_name' => 'things']);

        self::assertSame([], $this->commands);
    }

    public function testObjectsWithTheSameKeyAreCollapsedAndTheLastOneWins(): void
    {
        $this->respond(new Result([]));

        $this->repository->saveObjects(
            [new Thing('c1', 'a', 'old'), new Thing('c1', 'b', 'other'), new Thing('c1', 'a', 'new')],
            ['table_name' => 'things', 'key_attributes' => ['clientId', 'id']]
        );

        $sent = array_map(
            static fn (array $request): array => self::unmarshal($request['PutRequest']['Item']),
            $this->commands[0]['RequestItems']['things']
        );
        self::assertSame([['a', 'new'], ['b', 'other']], array_map(static fn (array $i): array => [$i['id'], $i['name']], $sent));
    }

    public function testCollapsingIsAcrossTheWholeInputNotJustOneBatch(): void
    {
        $this->respond(new Result([]), new Result([]));

        $things = $this->things(30);
        $things[] = new Thing('c1', 'id-0', 'Thing 0 again');   // same key as the first, 31st object

        $this->repository->saveObjects($things, ['table_name' => 'things', 'key_attributes' => ['clientId', 'id']]);

        self::assertCount(25, $this->commands[0]['RequestItems']['things']);
        self::assertCount(5, $this->commands[1]['RequestItems']['things'], '30 distinct keys, not 31 requests');
        self::assertSame('Thing 0 again', self::unmarshal($this->commands[0]['RequestItems']['things'][0]['PutRequest']['Item'])['name']);
    }

    public function testIdenticalObjectsAreCollapsedWithoutKeyAttributes(): void
    {
        $this->respond(new Result([]));

        $this->repository->saveObjects([new Thing('c1', 'a', 'x'), new Thing('c1', 'a', 'x')], ['table_name' => 'things']);

        self::assertCount(1, $this->commands[0]['RequestItems']['things']);
    }

    public function testAPutAndADeleteOfTheSameKeyKeepOnlyTheLast(): void
    {
        $this->respond(new Result([]));

        $this->repository->batchWrite(
            'things',
            [
                ['PutRequest' => ['Item' => self::marshal(['id' => 'a', 'name' => 'x'])]],
                ['DeleteRequest' => ['Key' => self::marshal(['id' => 'a'])]],
            ],
            ['key_attributes' => ['id']]
        );

        self::assertSame([['DeleteRequest' => ['Key' => ['id' => ['S' => 'a']]]]], $this->commands[0]['RequestItems']['things']);
    }

    public function testARequestWithoutAKeyAttributeIsRejectedBeforeAnythingIsSent(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('"clientId"');

        try {
            $this->repository->batchWrite(
                'things',
                [['PutRequest' => ['Item' => self::marshal(['id' => 'a'])]]],
                ['key_attributes' => ['clientId', 'id']]
            );
        } finally {
            self::assertSame([], $this->commands);
        }
    }

    public function testUnprocessedItemsAreRetriedUntilWritten(): void
    {
        $things = $this->things(3);
        $throttled = ['PutRequest' => ['Item' => self::marshal((array) $things[1])]];
        $this->respond(
            new Result(['UnprocessedItems' => ['things' => [$throttled]]]),
            new Result(['UnprocessedItems' => []])
        );

        $this->repository->saveObjects($things, ['table_name' => 'things']);

        self::assertCount(2, $this->commands);
        self::assertCount(3, $this->commands[0]['RequestItems']['things']);
        self::assertSame([$throttled], $this->commands[1]['RequestItems']['things'], 'only the unprocessed item is re-sent');
        self::assertCount(1, $this->repository->sleeps);
        self::assertSame('warning', $this->logger->records[0]['level']);
        self::assertSame(['table' => 'things', 'remaining' => 1, 'attempt' => 1], $this->logger->records[0]['context']);
    }

    public function testRetryWaitsAreJitteredUnderACappedExponentialCeiling(): void
    {
        $throttled = ['PutRequest' => ['Item' => self::marshal(['clientId' => 'c1', 'id' => 'a'])]];
        $unprocessed = new Result(['UnprocessedItems' => ['things' => [$throttled]]]);
        $this->respond($unprocessed, $unprocessed, $unprocessed, $unprocessed, $unprocessed, $unprocessed, $unprocessed, new Result([]));

        $this->repository->batchWrite('things', [$throttled]);

        self::assertCount(8, $this->commands, 'the default is 8 attempts');
        $ceilingsMs = [100, 200, 400, 800, 1600, 3200, 5000];    // 50 ms * 2^attempt, capped at 5 s
        self::assertCount(7, $this->repository->sleeps);
        foreach ($this->repository->sleeps as $i => $microseconds) {
            self::assertGreaterThanOrEqual(0, $microseconds);
            self::assertLessThanOrEqual($ceilingsMs[$i] * 1000, $microseconds, "wait before retry $i");
        }
    }

    public function testGivingUpThrowsWithEveryRequestThatWasNotWritten(): void
    {
        $things = $this->things(30);                              // batches of 25 and 5
        $leftovers = [
            ['PutRequest' => ['Item' => self::marshal((array) $things[3])]],
            ['PutRequest' => ['Item' => self::marshal((array) $things[7])]],
        ];
        $unprocessed = new Result(['UnprocessedItems' => ['things' => $leftovers]]);
        $this->respond($unprocessed, $unprocessed, $unprocessed);

        try {
            $this->repository->saveObjects($things, ['table_name' => 'things', 'max_attempts' => 3]);
            self::fail('Expected UnprocessedItemsException');
        } catch (UnprocessedItemsException $e) {
            self::assertInstanceOf(RuntimeException::class, $e, 'catchable like the holidays fork\'s RuntimeException');
            self::assertStringContainsString('unprocessed', $e->getMessage());
            self::assertSame('things', $e->getTableName());
            self::assertSame(3, $e->getAttempts());
            self::assertCount(7, $e->getUnprocessedItems(), '2 leftovers of batch 1 + the 5 of batch 2, never sent');
            self::assertSame($leftovers, \array_slice($e->getUnprocessedItems(), 0, 2));
        }

        self::assertCount(3, $this->commands, 'the second batch is not attempted after the first gives up');
    }

    public function testAnErrorFromBatchWriteItemPropagates(): void
    {
        $this->respond('ValidationException');

        $this->expectException(DynamoDbException::class);

        $this->repository->saveObjects($this->things(2), ['table_name' => 'things']);
    }

    public function testAnUnencodableObjectFailsLoudlyAfterTheOthersAreSaved(): void
    {
        $this->respond(new Result([]));

        try {
            $this->repository->saveObjects(
                [new Thing('c1', 'a'), new Thing('c1', 'bad', 'x', NAN), new Thing('c1', 'b')],
                ['table_name' => 'things']
            );
            self::fail('Expected UnencodableObjectsException');
        } catch (UnencodableObjectsException $e) {
            self::assertTrue($e->wereOthersWritten());
            self::assertSame([['index' => 1, 'class' => Thing::class, 'message' => $e->getPrevious()->getMessage()]], $e->getFailures());
            self::assertInstanceOf(NotEncodableValueException::class, $e->getPrevious());
        }

        self::assertCount(2, $this->commands[0]['RequestItems']['things'], 'the two encodable objects were written');
    }

    public function testThrowBeforeWriteSavesNothingWhenAnyObjectIsUnencodable(): void
    {
        try {
            $this->repository->saveObjects(
                [new Thing('c1', 'a'), new Thing('c1', 'bad', 'x', INF)],
                ['table_name' => 'things', 'on_unencodable' => 'throw_before_write']
            );
            self::fail('Expected UnencodableObjectsException');
        } catch (UnencodableObjectsException $e) {
            self::assertFalse($e->wereOthersWritten());
        }

        self::assertSame([], $this->commands);
    }

    /**
     * @return list<Thing>
     */
    private function things(int $count): array
    {
        $things = [];
        for ($i = 0; $i < $count; ++$i) {
            $things[] = new Thing('c1', "id-$i", "Thing $i");
        }

        return $things;
    }
}
