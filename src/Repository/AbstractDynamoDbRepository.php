<?php


namespace VictorMacko\Repository;


use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDb\Exception\DynamoDbException;
use Aws\DynamoDb\Marshaler;
use Aws\Result;
use Psr\Log\LoggerInterface;
use Symfony\Component\OptionsResolver\OptionsResolver;
use Symfony\Component\Serializer\SerializerInterface;

abstract class AbstractDynamoDbRepository
{
    /** @var DynamoDbClient */
    protected $dynamoDbClient;

    /** @var LoggerInterface */
    protected $logger;

    /** @var SerializerInterface */
    protected $serializer;

    /**
     * @param DynamoDbClient $dynamoDbClient
     * @param LoggerInterface $logger
     * @param SerializerInterface $serializer
     * @required
     */
    public function setServices(
        DynamoDbClient $dynamoDbClient,
        LoggerInterface $logger,
        SerializerInterface $serializer
    ) {
        $this->dynamoDbClient = $dynamoDbClient;
        $this->logger = $logger;
        $this->serializer = $serializer;
    }

    public function getOneOrNullById(array $options)
    {
        $resolver = new OptionsResolver();
        $resolver->setDefaults(
            [
                'table_name' => null,
                'key' => null,
                'class_name' => null,
            ]
        );
        $resolver->setAllowedTypes('key', 'array');
        $options = $resolver->resolve($options);

        try {
            $marshaller = new Marshaler();

            $result = $this->dynamoDbClient->getItem(
                [
                    'TableName' => $options['table_name'],
                    'Key' => $marshaller->marshalJson(json_encode($options['key'])),
                ]
            );

            if ($result['Item'] === null) {
                return null;
            }

            return $this->serializer->denormalize($marshaller->unmarshalItem($result['Item']), $options['class_name']);
        } catch (DynamoDbException $e) {
            return null;
        }
    }

    protected function convertResultsToObjectArray(Result $result, string $className)
    {
        $marshaler = new Marshaler();

        $ret = [];
        foreach ($result['Items'] as $row) {
            $rowData = $marshaler->unmarshalItem($row, false);

            /*
            $obj = $this->serializer->denormalize(
                $rowData,
                Member::class,
                null,
                [ObjectNormalizer::DISABLE_TYPE_ENFORCEMENT => true]
            );
            */
            $ret[] = $this->serializer->denormalize($rowData, $className);
        }

        return $ret;
    }

    protected function saveObject($object, array $options)
    {
        $resolver = new OptionsResolver();
        $resolver->setDefaults(
            [
                'table_name' => null,
            ]
        );
        $options = $resolver->resolve($options);

        $marshaler = new Marshaler();
        $json = $this->serializer->serialize($object, 'json');

        $this->dynamoDbClient->putItem(
            [
                'TableName' => $options['table_name'],
                'Item' => $marshaler->marshalJson($json),
            ]
        );
    }

    protected function deleteByKey(array $options)
    {
        $resolver = new OptionsResolver();
        $resolver->setDefaults(
            [
                'table_name' => null,
                'key' => null,
            ]
        );
        $resolver->setAllowedTypes('key', 'array');
        $options = $resolver->resolve($options);

        $marshaller = new Marshaler();

        $this->dynamoDbClient->deleteItem(
            [
                'TableName' => $options['table_name'],
                'Key' => $marshaller->marshalJson(json_encode($options['key'])),
            ]
        );
    }
}