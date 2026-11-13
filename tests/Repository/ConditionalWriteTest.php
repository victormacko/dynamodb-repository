<?php

declare(strict_types=1);

namespace VictorMacko\Tests\Repository;

use Aws\DynamoDb\Exception\DynamoDbException;
use Aws\Result;
use VictorMacko\Repository\Exception\ConditionFailed;
use VictorMacko\Tests\Fixtures\Thing;

/**
 * F4 (AP-06): saveObject()/deleteByKey() pass a ConditionExpression through, and a failed condition is ConditionFailed.
 */
final class ConditionalWriteTest extends MockDynamoDbTestCase
{
    public function testSaveObjectWithoutAConditionSendsThePutAsBefore(): void
    {
        $this->respond(new Result([]));

        $this->repository->saveObject(new Thing('c1', 'a', 'A'), ['table_name' => 'things']);

        self::assertSame('PutItem', $this->commands[0]->getName());
        self::assertSame(['clientId' => 'c1', 'id' => 'a', 'name' => 'A', 'score' => 1.5], self::unmarshal($this->commands[0]['Item']));
        foreach (['ConditionExpression', 'ExpressionAttributeNames', 'ExpressionAttributeValues'] as $param) {
            self::assertArrayNotHasKey($param, $this->commands[0]->toArray());
        }
    }

    public function testSaveObjectSendsTheConditionAndMarshalsItsValues(): void
    {
        $this->respond(new Result([]));

        $this->repository->saveObject(new Thing('c1', 'a'), [
            'table_name' => 'things',
            'condition_expression' => '#v = :seen',
            'expression_attribute_names' => ['#v' => 'version'],
            'expression_attribute_values' => [':seen' => 3],
        ]);

        self::assertSame('#v = :seen', $this->commands[0]['ConditionExpression']);
        self::assertSame(['#v' => 'version'], $this->commands[0]['ExpressionAttributeNames']);
        self::assertSame([':seen' => ['N' => '3']], $this->commands[0]['ExpressionAttributeValues']);
    }

    public function testAnEmptyValuesMapIsNotSent(): void
    {
        $this->respond(new Result([]));

        $this->repository->saveObject(new Thing('c1', 'a'), [
            'table_name' => 'things',
            'condition_expression' => 'attribute_not_exists(#id)',
            'expression_attribute_names' => ['#id' => 'id'],
        ]);

        self::assertArrayNotHasKey('ExpressionAttributeValues', $this->commands[0]->toArray(), 'DynamoDB rejects an empty map');
    }

    public function testAFailedConditionOnSaveThrowsConditionFailed(): void
    {
        $this->respond('ConditionalCheckFailedException');

        try {
            $this->repository->saveObject(new Thing('c1', 'a'), [
                'table_name' => 'things',
                'condition_expression' => 'attribute_not_exists(#id)',
                'expression_attribute_names' => ['#id' => 'id'],
            ]);
            self::fail('Expected ConditionFailed');
        } catch (ConditionFailed $e) {
            self::assertSame('things', $e->getTableName());
            self::assertInstanceOf(DynamoDbException::class, $e->getPrevious());
            self::assertSame('ConditionalCheckFailedException', $e->getPrevious()->getAwsErrorCode());
        }
    }

    public function testOtherErrorsOnAConditionalWritePropagateUnchanged(): void
    {
        $this->respond('ProvisionedThroughputExceededException');

        try {
            $this->repository->saveObject(new Thing('c1', 'a'), [
                'table_name' => 'things',
                'condition_expression' => 'attribute_not_exists(#id)',
                'expression_attribute_names' => ['#id' => 'id'],
            ]);
            self::fail('Expected a DynamoDbException');
        } catch (DynamoDbException $e) {
            self::assertSame('ProvisionedThroughputExceededException', $e->getAwsErrorCode());
        }
    }

    public function testDeleteByKeyWithoutAConditionSendsTheDeleteAsBefore(): void
    {
        $this->respond(new Result([]));

        $this->repository->deleteByKey(['table_name' => 'things', 'key' => ['clientId' => 'c1', 'id' => 'a']]);

        self::assertSame('DeleteItem', $this->commands[0]->getName());
        self::assertSame(['clientId' => ['S' => 'c1'], 'id' => ['S' => 'a']], $this->commands[0]['Key']);
        self::assertArrayNotHasKey('ConditionExpression', $this->commands[0]->toArray());
    }

    public function testDeleteByKeyMustExistThrowsConditionFailedWhenItDoesNot(): void
    {
        $this->respond('ConditionalCheckFailedException');

        try {
            $this->repository->deleteByKey([
                'table_name' => 'things',
                'key' => ['clientId' => 'c1', 'id' => 'a'],
                'condition_expression' => 'attribute_exists(#id)',
                'expression_attribute_names' => ['#id' => 'id'],
            ]);
            self::fail('Expected ConditionFailed');
        } catch (ConditionFailed $e) {
            self::assertSame('things', $e->getTableName());
        }

        self::assertSame('attribute_exists(#id)', $this->commands[0]['ConditionExpression']);
        self::assertSame(['#id' => 'id'], $this->commands[0]['ExpressionAttributeNames']);
    }

    public function testExpressionMapsWithoutAConditionAreRejectedBeforeSending(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        try {
            $this->repository->saveObject(new Thing('c1', 'a'), [
                'table_name' => 'things',
                'expression_attribute_names' => ['#id' => 'id'],
            ]);
        } finally {
            self::assertSame([], $this->commands);
        }
    }
}
