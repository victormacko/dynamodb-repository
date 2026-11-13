<?php

declare(strict_types=1);

namespace VictorMacko\Tests\Fixtures;

final class Thing
{
    public function __construct(
        public string $clientId = '',
        public string $id = '',
        public string $name = '',
        public float $score = 1.5,
    ) {
    }
}
