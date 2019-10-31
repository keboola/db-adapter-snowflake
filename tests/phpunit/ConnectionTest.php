<?php

declare(strict_types=1);

namespace Keboola\SnowflakeDbAdapter\Tests;

use Keboola\SnowflakeDbAdapter\Connection;
use Keboola\SnowflakeDbAdapter\Exception\BaseException;
use PHPUnit\Framework\TestCase;
use ReflectionClass;

class ConnectionTest extends TestCase
{
    public function testCanConnect(): void
    {
        $connection = new Connection([
            'host' => getenv('SNOWFLAKE_HOST'),
            'user' => getenv('SNOWFLAKE_USER'),
            'password' => getenv('SNOWFLAKE_PASSWORD'),
            'database' => getenv('SNOWFLAKE_DATABASE'),
        ]);
        $this->assertInstanceOf(Connection::class, $connection);
        $res = $connection->fetchAll('SELECT 1 AS "result"');
        $this->assertSame('1', $res[0]['result']);
    }

    public function testFailsToConnectWithUnknownParams(): void
    {
        $this->expectException(BaseException::class);
        $this->expectExceptionMessage('Unknown options: someRandomParameter, otherRandomParameter, 0');

        new Connection([
            'host' => getenv('SNOWFLAKE_HOST'),
            'user' => getenv('SNOWFLAKE_USER'),
            'password' => getenv('SNOWFLAKE_PASSWORD'),
            'someRandomParameter' => false,
            'otherRandomParameter' => false,
            'value',
        ]);
    }

    public function testFailsWhenRequiredParamsMissing(): void
    {
        $this->expectException(BaseException::class);
        $this->expectExceptionMessage('Missing options: user');

        new Connection([
            'host' => getenv('SNOWFLAKE_HOST'),
            'password' => getenv('SNOWFLAKE_PASSWORD'),
        ]);
    }

    public function testWillDisconnect(): void
    {
        $connection = new Connection([
            'host' => getenv('SNOWFLAKE_HOST'),
            'user' => getenv('SNOWFLAKE_USER'),
            'password' => getenv('SNOWFLAKE_PASSWORD'),
        ]);
        $reflection = new ReflectionClass($connection);
        $connectionProperty = $reflection->getProperty('connection');
        $connectionProperty->setAccessible(true);
        $this->assertIsResource($connectionProperty->getValue($connection));
        $this->assertSame('odbc link', get_resource_type($connectionProperty->getValue($connection)));
        $connection->disconnect();
        $this->assertSame('Unknown', get_resource_type($connectionProperty->getValue($connection)));
        $connectionProperty->setAccessible(false);
    }
}
