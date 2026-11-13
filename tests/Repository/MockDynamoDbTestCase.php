<?php

declare(strict_types=1);

namespace VictorMacko\Tests\Repository;

use Aws\CommandInterface;
use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDb\Exception\DynamoDbException;
use Aws\DynamoDb\Marshaler;
use Aws\MockHandler;
use Aws\Result;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Serializer\Encoder\JsonEncoder;
use Symfony\Component\Serializer\Normalizer\ObjectNormalizer;
use Symfony\Component\Serializer\Serializer;
use VictorMacko\Tests\Fixtures\RecordingLogger;
use VictorMacko\Tests\Fixtures\TestRepository;

/**
 * Runs the real repository code against the AWS SDK's MockHandler: no network, and every command sent is recorded.
 * The client is built with 'retries' => 0 so a throttling error reaches the repository at once.
 */
abstract class MockDynamoDbTestCase extends TestCase
{
    protected MockHandler $mock;

    protected RecordingLogger $logger;

    protected TestRepository $repository;

    /** @var list<CommandInterface> */
    protected array $commands = [];

    protected function setUp(): void
    {
        $this->mock = new MockHandler();
        $this->logger = new RecordingLogger();
        $client = new DynamoDbClient([
            'region' => 'ap-southeast-2',
            'version' => 'latest',
            'credentials' => ['key' => 'test', 'secret' => 'test'],
            'retries' => 0,
            'handler' => $this->mock,
        ]);

        $this->repository = new TestRepository();
        $this->repository->setServices($client, $this->logger, new Serializer([new ObjectNormalizer()], [new JsonEncoder()]));
    }

    /**
     * Queue responses (Result or DynamoDbException error codes) and record the command each one answers.
     */
    protected function respond(Result|string ...$responses): void
    {
        foreach ($responses as $response) {
            $this->mock->append(function (CommandInterface $command) use ($response) {
                $this->commands[] = $command;

                return \is_string($response)
                    ? new DynamoDbException("Mocked $response", $command, ['code' => $response])
                    : $response;
            });
        }
    }

    protected static function marshal(array $item): array
    {
        return (new Marshaler())->marshalItem($item);
    }

    protected static function unmarshal(array $item): array
    {
        return (new Marshaler())->unmarshalItem($item);
    }
}
