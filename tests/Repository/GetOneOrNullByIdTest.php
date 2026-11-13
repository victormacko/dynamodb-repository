<?php

declare(strict_types=1);

namespace VictorMacko\Tests\Repository;

use Aws\DynamoDb\Exception\DynamoDbException;
use Aws\Result;
use PHPUnit\Framework\Attributes\DataProvider;
use VictorMacko\Repository\DynamoDbRepositoryHelper;
use VictorMacko\Tests\Fixtures\Thing;

/**
 * F1 (AP-04) and F5: null means "no item", nothing else.
 */
final class GetOneOrNullByIdTest extends MockDynamoDbTestCase
{
    public function testReturnsNullWhenTheResponseHasNoItem(): void
    {
        $this->respond(new Result([]));

        self::assertNull($this->repository->getOneOrNullById($this->options()));

        self::assertCount(1, $this->commands);
        self::assertSame('GetItem', $this->commands[0]->getName());
        self::assertSame('things', $this->commands[0]['TableName']);
        self::assertSame(['clientId' => ['S' => 'c1'], 'id' => ['S' => '42-c1']], $this->commands[0]['Key']);
        self::assertFalse($this->commands[0]['ConsistentRead'], 'eventually consistent unless asked');
    }

    public function testDenormalizesTheItem(): void
    {
        $this->respond(new Result(['Item' => self::marshal(['clientId' => 'c1', 'id' => '42-c1', 'name' => 'Stage', 'score' => 2.5])]));

        $thing = $this->repository->getOneOrNullById($this->options());

        self::assertInstanceOf(Thing::class, $thing);
        self::assertSame('Stage', $thing->name);
        self::assertSame(2.5, $thing->score);
    }

    public function testConsistentReadIsPassedThrough(): void
    {
        $this->respond(new Result([]));

        $this->repository->getOneOrNullById($this->options(['consistent_read' => true]));

        self::assertTrue($this->commands[0]['ConsistentRead']);
    }

    public function testKeyValuesKeepTheirPhpType(): void
    {
        $this->respond(new Result([]));

        $this->repository->getOneOrNullById($this->options(['key' => ['clientId' => 'c1', 'id' => 42]]));

        self::assertSame(['clientId' => ['S' => 'c1'], 'id' => ['N' => '42']], $this->commands[0]['Key']);
    }

    #[DataProvider('errorCodes')]
    public function testDynamoDbErrorsPropagateInsteadOfLookingLikeNotFound(string $code): void
    {
        $this->respond($code);

        try {
            $this->repository->getOneOrNullById($this->options());
            self::fail('Expected a DynamoDbException, got a return value');
        } catch (DynamoDbException $e) {
            self::assertSame($code, $e->getAwsErrorCode());
        }
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function errorCodes(): iterable
    {
        yield 'throttled after the SDK retries' => ['ProvisionedThroughputExceededException'];
        yield 'IAM' => ['AccessDeniedException'];
        yield 'wrong table or stage' => ['ResourceNotFoundException'];
        yield 'key of the wrong type' => ['ValidationException'];
    }

    public function testTheHelperStillOverridesTheBaseCompatibly(): void
    {
        // DynamoDbRepositoryHelper re-declares protected methods as public; an incompatible signature would be a fatal error here.
        self::assertInstanceOf(DynamoDbRepositoryHelper::class, new DynamoDbRepositoryHelper());
    }

    private function options(array $overrides = []): array
    {
        return $overrides + [
            'table_name' => 'things',
            'key' => ['clientId' => 'c1', 'id' => '42-c1'],
            'class_name' => Thing::class,
        ];
    }
}
