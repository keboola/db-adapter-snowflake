<?php

declare(strict_types=1);

namespace Keboola\SnowflakeDbAdapter\Tests;

use Keboola\SnowflakeDbAdapter\Connection;
use Keboola\SnowflakeDbAdapter\Exception\SnowflakeDbAdapterException;
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
        $this->expectException(SnowflakeDbAdapterException::class);
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
        $this->expectException(SnowflakeDbAdapterException::class);
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

    public function testQueryTagging(): void
    {
        $connection = new Connection([
            'host' => getenv('SNOWFLAKE_HOST'),
            'user' => getenv('SNOWFLAKE_USER'),
            'password' => getenv('SNOWFLAKE_PASSWORD'),
            'database' => getenv('SNOWFLAKE_DATABASE'),
            'warehouse' => getenv('SNOWFLAKE_WAREHOUSE'),
            'runId' => 'runIdValue',
        ]);

        $connection->fetchAll('SELECT current_date;');
        $queries = $connection->fetchAll(
            '
                SELECT 
                    QUERY_TEXT, QUERY_TAG 
                FROM 
                    TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION())
                WHERE QUERY_TEXT = \'SELECT current_date;\' 
                ORDER BY START_TIME DESC 
                LIMIT 1
            '
        );

        $this->assertEquals('{"runId":"runIdValue"}', $queries[0]['QUERY_TAG']);
    }
}
