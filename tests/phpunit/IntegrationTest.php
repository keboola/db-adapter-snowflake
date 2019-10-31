<?php

declare(strict_types=1);

namespace Keboola\SnowflakeDbAdapter\Tests;

use Keboola\SnowflakeDbAdapter\Connection;
use Keboola\SnowflakeDbAdapter\Exception\StringTooLongException;
use Keboola\SnowflakeDbAdapter\Exception\WarehouseTimeoutReached;
use PHPUnit\Framework\TestCase;

class IntegrationTest extends TestCase
{
    /**
     * @var Connection
     */
    private $connection;

    /** @var string */
    private $destSchemaName = 'in.c-tests';

    /** @var string */
    private $sourceSchemaName = 'some.tests';

    public function setUp(): void
    {
        $this->connection = new Connection([
            'host' => getenv('SNOWFLAKE_HOST'),
            'user' => getenv('SNOWFLAKE_USER'),
            'password' => getenv('SNOWFLAKE_PASSWORD'),
            'database' => getenv('SNOWFLAKE_DATABASE'),
            'warehouse' => getenv('SNOWFLAKE_WAREHOUSE'),
        ]);
        $this->connection->query('SELECT 1');
        $this->initData();
    }

    protected function tearDown(): void
    {
        unset($this->connection);
    }

    /**
     * This should not exhaust memory
     */
    public function testLargeTableIterate(): void
    {
        $generateRowsCount = 1000000;
        $connection = $this->connection;
        $connection->query(sprintf('
          CREATE TABLE "%s"."bigData" AS
            SELECT 
              uniform(1, 10, random()) AS "col1",
              uniform(1, 10, random()) AS "col2",
              uniform(1, 10, random()) AS "col3"
            FROM TABLE(GENERATOR(rowCount => %s)) v ORDER BY 1;
        ', $this->destSchemaName, $generateRowsCount));
        $results = [
            'count' => 0,
        ];
        $callback = function ($row) use (&$results): void {
            $results['count'] = $results['count'] + 1;
        };
        $connection->fetch(sprintf(
            'SELECT * FROM %s.%s',
            $connection->quoteIdentifier($this->destSchemaName),
            $connection->quoteIdentifier('bigData')
        ), [], $callback);
        $this->assertEquals($generateRowsCount, $results['count']);
    }

    public function testTableInfo(): void
    {
        $destTableName = 'Test';
        $this->connection->query(sprintf(
            "CREATE TABLE \"%s\".\"%s\" (
                col1 int NOT NULL DEFAULT 7 COMMENT 'SomeComment', 
                \"col2\" varchar(255)
            )",
            $this->destSchemaName,
            $destTableName
        ));
        $table = $this->connection->describeTable($this->destSchemaName, $destTableName);
        $this->assertEquals($destTableName, $table['name']);
        $this->assertArrayHasKey('rows', $table);
        $this->assertArrayHasKey('bytes', $table);

        $columns = $this->connection->getTableColumns($this->destSchemaName, $destTableName);
        $this->assertSame(['COL1', 'col2'], $columns);

        $columnsMetadata = $this->connection->describeTableColumns($this->destSchemaName, $destTableName);
        $this->assertCount(2, $columnsMetadata);
        $expectedFirstColumn = [
            'table_name' => $destTableName,
            'schema_name' => $this->destSchemaName,
            'column_name' => 'COL1',
            'data_type' => '{"type":"FIXED","precision":38,"scale":0,"nullable":false}',
            'null?' => 'NOT_NULL',
            'default' => '7',
            'kind' => 'COLUMN',
            'expression' => '',
            'comment' => 'SomeComment',
            'autoincrement' => '',
        ];
        unset($columnsMetadata[0]['database_name']);
        $this->assertSame($expectedFirstColumn, $columnsMetadata[0]);
    }

    public function testGetPrimaryKey(): void
    {
        $pk = $this->connection->getTablePrimaryKey($this->destSchemaName, 'accounts-3');
        $this->assertEquals(['id'], $pk);
    }

    private function initData(): void
    {
        foreach ([$this->sourceSchemaName, $this->destSchemaName] as $schema) {
            $this->connection->query(sprintf('DROP SCHEMA IF EXISTS "%s"', $schema));
            $this->connection->query(sprintf('CREATE SCHEMA "%s"', $schema));
        }

        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."accounts-3" (
                "id" varchar(65535) NOT NULL,
                "idTwitter" varchar(65535) NOT NULL,
                "name" varchar(65535) NOT NULL,
                "import" varchar(65535) NOT NULL,
                "isImported" varchar(65535) NOT NULL,
                "apiLimitExceededDatetime" varchar(65535) NOT NULL,
                "analyzeSentiment" varchar(65535) NOT NULL,
                "importKloutScore" varchar(65535) NOT NULL,
                "timestamp" varchar(65535) NOT NULL,
                "oauthToken" varchar(65535) NOT NULL,
                "oauthSecret" varchar(65535) NOT NULL,
                "idApp" varchar(65535) NOT NULL,
                "_timestamp" TIMESTAMP_NTZ,
                PRIMARY KEY("id")
            )',
            $this->destSchemaName
        ));
    }

    public function testConnectionBinding(): void
    {
        $connection = $this->connection;
        $destSchemaName = 'test';
        $this->prepareSchema($connection, $destSchemaName);
        $connection->query('CREATE TABLE "' . $destSchemaName . '"."Test" (col1 varchar, col2 varchar)');
        $connection->query('INSERT INTO "' . $destSchemaName . '"."Test" VALUES (\'\\\'a\\\'\',\'b\')');
        $connection->query('INSERT INTO "' . $destSchemaName . '"."Test" VALUES (\'a\',\'b\')');
        $rows = $connection->fetchAll('SELECT * FROM "' . $destSchemaName . '"."Test" WHERE col1 = ?', ["'a'"]);
        $this->assertEmpty($rows);
    }

    public function testConnectionEncoding(): void
    {
        $connection = $this->connection;
        $destSchemaName = 'test';
        $this->prepareSchema($connection, $destSchemaName);
        $connection->query('CREATE TABLE "' . $destSchemaName . '"."TEST" (col1 varchar, col2 varchar)');
        $connection->query(
            'INSERT INTO  "' . $destSchemaName . '"."TEST" VALUES (\'šperky.cz\', \'módní doplňky.cz\')'
        );
        $data = $connection->fetchAll('SELECT * FROM "' . $destSchemaName . '"."TEST"');
        $this->assertEquals([
            [
                'COL1' => 'šperky.cz',
                'COL2' => 'módní doplňky.cz',
            ],
        ], $data);
    }

    public function testTooLargeColumnInsert(): void
    {
        $connection = $this->connection;
        $destSchemaName = 'test';
        $this->prepareSchema($connection, $destSchemaName);
        $size = 10;
        $connection->query(
            sprintf(
                'CREATE TABLE "%s"."%s" ("col1" varchar(%d));',
                $destSchemaName,
                'TEST',
                $size
            )
        );
        $this->expectException(StringTooLongException::class);
        $this->expectExceptionMessageRegExp('/cannot be inserted because it\'s bigger than column size/');
        $connection->query(
            sprintf(
                'INSERT INTO "%s"."%s" VALUES(\'%s\');',
                $destSchemaName,
                'TEST',
                implode('', array_fill(0, $size + 1, 'x'))
            )
        );
    }

    public function testQueryTimeoutLimit(): void
    {
        $connection = $this->connection;
        $connection->query('ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 3');
        try {
            $connection->fetchAll('CALL system$wait(5)');
        } catch (WarehouseTimeoutReached $e) {
            $this->assertSame(WarehouseTimeoutReached::class, get_class($e));
            $this->assertSame('Query reached its timeout 3 second(s)', $e->getMessage());
        } finally {
            $connection->query('ALTER SESSION UNSET STATEMENT_TIMEOUT_IN_SECONDS');
        }
    }

    private function prepareSchema(Connection $connection, string $schemaName): void
    {
        $connection->query(sprintf('DROP SCHEMA IF EXISTS "%s"', $schemaName));
        $connection->query(sprintf('CREATE SCHEMA "%s"', $schemaName));
    }
}
