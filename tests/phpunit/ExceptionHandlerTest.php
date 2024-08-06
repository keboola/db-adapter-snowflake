<?php

declare(strict_types=1);

namespace Keboola\SnowflakeDbAdapter\Tests;

use Exception;
use InvalidArgumentException;
use Keboola\SnowflakeDbAdapter\Exception\RuntimeException;
use Keboola\SnowflakeDbAdapter\Exception\SnowflakeDbAdapterException;
use Keboola\SnowflakeDbAdapter\Exception\StringTooLongException;
use Keboola\SnowflakeDbAdapter\Exception\WarehouseTimeoutReached;
use Keboola\SnowflakeDbAdapter\ExceptionHandler;
use PHPUnit\Framework\TestCase;
use Throwable;

class ExceptionHandlerTest extends TestCase
{

    /**
     * @dataProvider provideExceptions
     */
    public function testHandleException(
        string $expectedExceptionClass,
        string $expectedExceptionMessage,
        Throwable $thrownException,
        ?string $sql = null,
    ): void {
        $handler = new ExceptionHandler();
        $this->expectException($expectedExceptionClass);
        $this->expectExceptionMessage($expectedExceptionMessage);
        $handler->handleException($thrownException, $sql);
    }

    public function provideExceptions(): array
    {
        return [
            'string too long' => [
                StringTooLongException::class,
                'String \'Lorem ipsum\' cannot be inserted because it\'s bigger than column size',
                new Exception('SQL001: String \'Lorem ipsum\' is too long for column "xy" SQL state 22000. Failed.'),
            ],
            'warehouse timeout reached' => [
                WarehouseTimeoutReached::class,
                'Query reached its timeout 800 second(s)',
                new Exception('Statement reached its statement or warehouse timeout of 800 seconds SQL state 57014'),
            ],
            'random exception' => [
                SnowflakeDbAdapterException::class,
                'Invalid argument "x"',
                new InvalidArgumentException('Invalid argument "x"'),
            ],
            'Runtime exception with SQL query' => [
                RuntimeException::class,
                'Error "No such table "test"" while executing query "INSERT INTO test VALUES (1), (2)"',
                new RuntimeException('No such table "test"'),
                'INSERT INTO test VALUES (1), (2)',
            ],
        ];
    }
}
