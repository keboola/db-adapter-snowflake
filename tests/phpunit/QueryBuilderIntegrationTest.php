<?php

declare(strict_types=1);

namespace Keboola\SnowflakeDbAdapter\Tests;

use InvalidArgumentException;
use Keboola\SnowflakeDbAdapter\Connection;
use Keboola\SnowflakeDbAdapter\Exception\RuntimeException;
use Keboola\SnowflakeDbAdapter\QueryBuilder;
use PHPUnit\Framework\TestCase;

class QueryBuilderIntegrationTest extends TestCase
{
    /** @var Connection */
    private $db;

    /** @dataProvider provideQuoteInput */
    public function testQuote(string $input): void
    {
        $quoted = QueryBuilder::quote($input);
        $result = $this->getConnection()->fetchAll('SELECT ' . $quoted . ' as "value"');
        $this->assertSame($input, $result[0]['value']);
    }

    public function provideQuoteInput(): array
    {
        return [
            'simple' => [
                'quote_name',
            ],
            'double quote' => [
                'quote"name"',
            ],
            'single quote' => [
                "quote'name'",
            ],
            'backslash' => [
                'quote\\name\\',
            ],
            'newline' => [
                "quote\nname\n",
            ],
            'forged to end string early' => [
                'quoted\\',
            ],
        ];
    }

    /** @dataProvider provideQuoteIdentifierInput */
    public function testQuoteIdentifier(string $input): void
    {
        $quoted = QueryBuilder::quoteIdentifier($input);
        $result = $this->getConnection()->fetchAll('SELECT \'a\' as ' . $quoted);
        $this->assertArrayHasKey($input, $result[0]);
    }

    public function provideQuoteIdentifierInput(): array
    {
        return [
            'simple' => [
                'quote_name',
            ],
            'single quote' => [
                "quote'name'",
            ],
            'backslash' => [
                'quote\\name\\',
            ],
            'newline' => [
                "quote\nname\n",
            ],
            'forged to end string early' => [
                'quoted\\',
            ],
        ];
    }

    public function testIdentifierCannotContainDoubleQuote(): void
    {
//        $this->markTestSkipped('Just to make sure it actually works like this');
        try {
            $this->getConnection()->query('SELECT \'a\' as "Double""Quotes"');
        } catch (\Throwable $e) {
            $this->assertInstanceOf(RuntimeException::class, $e);
            $expectedMessage = 'Error "odbc_prepare(): SQL error: '
                . 'SQL compilation error:' . "\n"
                . 'syntax error line 1 at position 22 unexpected \'"Quotes"\'., '
                . 'SQL state 37000 in SQLPrepare" while executing query '
                . '"SELECT \'a\' as "Double""Quotes""';
            $this->assertSame(
                $expectedMessage,
                $e->getMessage()
            );
        }
    }

    public function getConnection(): Connection
    {
        if (!$this->db instanceof Connection) {
            $this->db = new Connection([
                'host' => $this->getEnv('SNOWFLAKE_HOST'),
                'database' => $this->getEnv('SNOWFLAKE_DATABASE'),
                'user' => $this->getEnv('SNOWFLAKE_USER'),
                'password' => $this->getEnv('SNOWFLAKE_PASSWORD'),
                'warehouse' => $this->getEnv('SNOWFLAKE_WAREHOUSE'),
            ]);
        }
        return $this->db;
    }

    public function getEnv(string $env): string
    {
        $value = getenv($env);
        if ($value === false) {
            throw new InvalidArgumentException(sprintf('Env value "%s" must be set', $env));
        }
        return $value;
    }
}
