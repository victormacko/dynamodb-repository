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
                'denormalize_context' => [],
            ]
        );
        $resolver->setAllowedTypes('table_name', 'string');
        $resolver->setAllowedTypes('key', 'array');
        $resolver->setAllowedTypes('class_name', 'string');
        $resolver->setAllowedTypes('denormalize_context', 'array');
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

            return $this->serializer->denormalize(
                $marshaller->unmarshalItem($result['Item']),
                $options['class_name'],
                null,
                $options['denormalize_context']
            );
        } catch (DynamoDbException $e) {
            return null;
        }
    }

    protected function convertResultsToObjectArray(Result $result, string $className, array $options = [])
    {
        $resolver = new OptionsResolver();
        $resolver->setDefaults(
            [
                'denormalize_context' => [],
            ]
        );
        $resolver->setAllowedTypes('denormalize_context', 'array');
        $options = $resolver->resolve($options);

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
            $ret[] = $this->serializer->denormalize($rowData, $className, null, $options['denormalize_context']);
        }

        return $ret;
    }

    protected function saveObject($object, array $options)
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
        $options = $resolver->resolve($options);

        $marshaler = new Marshaler();
        $json = $this->serializer->serialize($object, 'json', $options['serialize_context']);

        $this->dynamoDbClient->putItem(
            [
                'TableName' => $options['table_name'],
                'Item' => $marshaler->marshalJson($json),
            ]
        );
    }

    protected function saveObjects(array $objects, array $options)
    {
        $resolver = new OptionsResolver();
        $resolver->setDefaults(
            [
                'table_name' => null,
                'serialize_context' => ['json_encode_options' => JSON_INVALID_UTF8_IGNORE],
            ]
        );
        $resolver->setAllowedTypes('table_name', 'string');
        $resolver->setAllowedTypes('serialize_context', 'array');
        $options = $resolver->resolve($options);

        $marshaler = new Marshaler();

        foreach (array_chunk($objects, 25) as $objChunk) {
            $items = [];
            foreach ($objChunk as $object) {
                try {
                    $json = $this->serializer->serialize($object, 'json', $options['serialize_context']);
                    $items[] = [
                        'PutRequest' => [
                            'Item' => $marshaler->marshalJson($json),
                        ],
                    ];
                } catch (NotEncodableValueException $e) {
                    $this->logger->error('Cannot run saveObjects - '.get_class($object).', '.$e->getMessage());
                }
            }

            if (count($items) > 0) {
                $this->dynamoDbClient->batchWriteItem(
                    [
                        'RequestItems' => [
                            $options['table_name'] => $items,
                        ],
                    ]
                );
            }
        }
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
        $resolver->setAllowedTypes('table_name', 'string');
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