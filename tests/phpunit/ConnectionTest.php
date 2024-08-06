<?php

declare(strict_types=1);

namespace Keboola\SnowflakeDbAdapter\Tests;

use Keboola\SnowflakeDbAdapter\Connection;
use Keboola\SnowflakeDbAdapter\Exception\CannotAccessObjectException;
use Keboola\SnowflakeDbAdapter\Exception\SnowflakeDbAdapterException;
use Keboola\SnowflakeDbAdapter\QueryBuilder;
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

    public function testInvalidAccessToDatabase(): void
    {
        $invalidDatabase = 'invalidDatabase';
        $config = [
            'host' => getenv('SNOWFLAKE_HOST'),
            'user' => getenv('SNOWFLAKE_USER'),
            'password' => getenv('SNOWFLAKE_PASSWORD'),
            'database' => $invalidDatabase,
        ];
        $connection = new Connection($config);

        $this->expectException(CannotAccessObjectException::class);
        $this->expectExceptionMessage(
            sprintf(
                'Cannot access object or it does not exist. Executing query "USE DATABASE %s"',
                QueryBuilder::quoteIdentifier($invalidDatabase),
            ),
        );
        $connection->query(sprintf('USE DATABASE %s', QueryBuilder::quoteIdentifier($invalidDatabase)));
    }

    public function testInvalidAccessToSchema(): void
    {
        $invalidSchema = 'invalidSchema';
        $config = [
            'host' => getenv('SNOWFLAKE_HOST'),
            'user' => getenv('SNOWFLAKE_USER'),
            'password' => getenv('SNOWFLAKE_PASSWORD'),
            'database' => getenv('SNOWFLAKE_DATABASE'),
            'schema' => $invalidSchema,
        ];
        $connection = new Connection($config);
        $this->expectException(CannotAccessObjectException::class);
        $this->expectExceptionMessage(
            sprintf(
                'Cannot access object or it does not exist. Executing query "USE SCHEMA %s"',
                QueryBuilder::quoteIdentifier($invalidSchema),
            ),
        );
        $connection->query(sprintf('USE SCHEMA %s', QueryBuilder::quoteIdentifier($invalidSchema)));
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
            ',
        );

        $this->assertEquals('{"runId":"runIdValue"}', $queries[0]['QUERY_TAG']);
    }
}
