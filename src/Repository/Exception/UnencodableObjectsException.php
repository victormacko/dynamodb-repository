<?php

namespace VictorMacko\Repository\Exception;

use Symfony\Component\Serializer\Exception\NotEncodableValueException;

/**
 * saveObjects() could not serialize some of the objects it was given, so they were not saved.
 *
 * Whether the other objects were saved depends on the on_unencodable option; see wereOthersWritten().
 * The previous exception is the first object's NotEncodableValueException.
 */
class UnencodableObjectsException extends \RuntimeException
{
    /** @var string */
    private $tableName;

    /** @var list<array{index: int|string, class: string, message: string}> */
    private $failures;

    /** @var bool */
    private $othersWritten;

    /**
     * @param list<array{index: int|string, class: string, exception: NotEncodableValueException}> $failures
     */
    public function __construct(string $tableName, array $failures, bool $othersWritten)
    {
        $first = $failures[0];
        parent::__construct(
            sprintf(
                '%d object(s) could not be encoded for "%s" and were not saved (%s); the other objects %s. First: %s at index %s: %s',
                count($failures),
                $tableName,
                implode(', ', array_unique(array_column($failures, 'class'))),
                $othersWritten ? 'were saved' : 'were not saved either',
                $first['class'],
                $first['index'],
                $first['exception']->getMessage()
            ),
            0,
            $first['exception']
        );
        $this->tableName = $tableName;
        $this->othersWritten = $othersWritten;
        $this->failures = array_map(
            static function (array $failure): array {
                return [
                    'index' => $failure['index'],
                    'class' => $failure['class'],
                    'message' => $failure['exception']->getMessage(),
                ];
            },
            $failures
        );
    }

    public function getTableName(): string
    {
        return $this->tableName;
    }

    /**
     * @return list<array{index: int|string, class: string, message: string}> index = the object's key in the input array
     */
    public function getFailures(): array
    {
        return $this->failures;
    }

    public function wereOthersWritten(): bool
    {
        return $this->othersWritten;
    }
}
