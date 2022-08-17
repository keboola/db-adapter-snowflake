<?php

declare(strict_types=1);

namespace Keboola\SnowflakeDbAdapter\Tests;

use Keboola\SnowflakeDbAdapter\Connection;
use Keboola\SnowflakeDbAdapter\Connection\Expr;
use Keboola\SnowflakeDbAdapter\QueryBuilder;
use PHPUnit\Framework\TestCase;

class QueryBuilderTest extends TestCase
{
    private QueryBuilder $queryBuilder;

    protected function setUp(): void
    {
        $this->queryBuilder = new QueryBuilder();
    }

    public function testDescribeTable(): void
    {
        $expected = 'DESC TABLE "schema""na\'me"."table""na\'me"';
        $this->assertSame(
            $expected,
            $this->queryBuilder->describeTable(
                'schema"na\'me',
                'table"na\'me'
            )
        );
    }

    public function testShowTableInSchema(): void
    {
        $expected = 'SHOW TABLES LIKE \'table\\"na\\\'me\' IN SCHEMA "schema""na\'me"';
        $this->assertSame(
            $expected,
            $this->queryBuilder->showTableInSchema(
                'schema"na\'me',
                'table"na\'me'
            )
        );
    }

    public function testShowColumns(): void
    {
        $expected = 'SHOW COLUMNS IN "schema""na\'me"."table""na\'me"';
        $this->assertSame(
            $expected,
            $this->queryBuilder->showColumns(
                'schema"na\'me',
                'table"na\'me'
            )
        );
    }

    /**
     * @dataProvider provideQuotingData
     */
    public function testQuoting(string $expected, string|Expr $input): void
    {
        $this->assertSame($expected, QueryBuilder::quote($input));
    }

    public function provideQuotingData(): array
    {
        return [
            'Expression' => [
                'COUNT(*)',
                new Connection\Expr('COUNT(*)'),
            ],
            'Single quote' => [
                "'lorem\'ipsum'",
                "lorem'ipsum",
            ],
        ];
    }

    /**
     * @dataProvider provideIdentifierQuotingData
     */
    public function testIdentifierQuoting(string $expected, string|Expr $input): void
    {
        $this->assertSame($expected, QueryBuilder::quoteIdentifier($input));
    }

    public function provideIdentifierQuotingData(): array
    {
        return [
            'Expression' => [
                'COUNT(*)',
                new Connection\Expr('COUNT(*)'),
            ],
            'Double quote' => [
                '"lorem\'ipsum"',
                "lorem'ipsum",
            ],
        ];
    }
}
