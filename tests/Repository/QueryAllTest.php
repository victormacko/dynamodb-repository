<?php

declare(strict_types=1);

namespace VictorMacko\Tests\Repository;

use Aws\DynamoDb\Exception\DynamoDbException;
use Aws\Result;
use VictorMacko\Repository\Exception\ResultTooLarge;
use VictorMacko\Tests\Fixtures\Thing;

/**
 * F3 (AP-01, AP-02): queryAll() reads every page, stops only when LastEvaluatedKey is absent, and is bounded.
 */
final class QueryAllTest extends MockDynamoDbTestCase
{
    public function testFollowsLastEvaluatedKeyUntilItIsAbsentIncludingAnEmptyPage(): void
    {
        $lek1 = ['clientId' => ['S' => 'c1'], 'id' => ['S' => 'b']];
        $lek2 = ['clientId' => ['S' => 'c1'], 'id' => ['S' => 'b2']];
        $this->respond(
            new Result(['Items' => [self::marshal(['id' => 'a']), self::marshal(['id' => 'b'])], 'LastEvaluatedKey' => $lek1]),
            new Result(['Items' => [], 'LastEvaluatedKey' => $lek2]),   // e.g. everything filtered out: not the end
            new Result(['Items' => [self::marshal(['id' => 'c'])]])
        );

        $items = $this->repository->queryAll($this->params());

        self::assertSame([['id' => 'a'], ['id' => 'b'], ['id' => 'c']], $items);
        self::assertCount(3, $this->commands);
        self::assertSame('Query', $this->commands[0]->getName());
        self::assertArrayNotHasKey('ExclusiveStartKey', $this->commands[0]->toArray());
        self::assertSame($lek1, $this->commands[1]['ExclusiveStartKey']);
        self::assertSame($lek2, $this->commands[2]['ExclusiveStartKey']);
    }

    public function testThrowsInsteadOfReadingPastMaxItems(): void
    {
        $this->respond(
            new Result(['Items' => [self::marshal(['id' => 'a']), self::marshal(['id' => 'b'])], 'LastEvaluatedKey' => ['id' => ['S' => 'b']]]),
            new Result(['Items' => [self::marshal(['id' => 'c'])], 'LastEvaluatedKey' => ['id' => ['S' => 'c']]])
        );

        $this->expectException(ResultTooLarge::class);
        $this->expectExceptionMessage('Query on "things" matched more than 2 items');

        $this->repository->queryAll($this->params(), 2);
    }

    public function testExactlyMaxItemsIsAllowed(): void
    {
        $this->respond(new Result(['Items' => [self::marshal(['id' => 'a']), self::marshal(['id' => 'b'])]]));

        self::assertCount(2, $this->repository->queryAll($this->params(), 2));
    }

    public function testErrorsPropagate(): void
    {
        $this->respond('ProvisionedThroughputExceededException');

        $this->expectException(DynamoDbException::class);

        $this->repository->queryAll($this->params());
    }

    public function testItemsCanBeDenormalizedIntoObjects(): void
    {
        $this->respond(new Result(['Items' => [self::marshal(['clientId' => 'c1', 'id' => 'a', 'name' => 'A', 'score' => 2.5])]]));

        $things = $this->repository->denormalizeItems($this->repository->queryAll($this->params()), Thing::class);

        self::assertEquals([new Thing('c1', 'a', 'A', 2.5)], $things);
    }

    public function testConvertResultsToObjectArrayStillMapsOnePage(): void
    {
        $things = $this->repository->convertResultsToObjectArray(
            new Result(['Items' => [self::marshal(['clientId' => 'c1', 'id' => 'a', 'name' => 'A', 'score' => 2.5])]]),
            Thing::class
        );

        self::assertEquals([new Thing('c1', 'a', 'A', 2.5)], $things);
    }

    private function params(): array
    {
        return [
            'TableName' => 'things',
            'KeyConditionExpression' => '#clientId = :clientId',
            'ExpressionAttributeNames' => ['#clientId' => 'clientId'],
            'ExpressionAttributeValues' => self::marshal([':clientId' => 'c1']),
        ];
    }
}
