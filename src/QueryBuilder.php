<?php

declare(strict_types=1);

namespace Keboola\SnowflakeDbAdapter;

use Keboola\SnowflakeDbAdapter\Connection\Expr;

class QueryBuilder
{
    public static function quote(string|Expr $value): string
    {
        if ($value instanceof Expr) {
            return $value->getValue();
        }
        $q = "'";
        return $q . addslashes($value) . $q;
    }

    public static function quoteIdentifier(string|Expr $value): string
    {
        if ($value instanceof Expr) {
            return $value->getValue();
        }
        $q = '"';
        return ($q . str_replace("$q", "$q$q", $value) . $q);
    }

    public function showTableInSchema(string $schemaName, string $tableName): string
    {
        return sprintf(
            'SHOW TABLES LIKE %s IN SCHEMA %s',
            "'" . addslashes($tableName) . "'",
            self::quoteIdentifier($schemaName),
        );
    }

    public function showColumns(string $schemaName, string $tableName): string
    {
        return sprintf(
            'SHOW COLUMNS IN %s.%s',
            self::quoteIdentifier($schemaName),
            self::quoteIdentifier($tableName),
        );
    }

    public function describeTable(string $schemaName, string $tableName): string
    {
        return sprintf(
            'DESC TABLE %s.%s',
            self::quoteIdentifier($schemaName),
            self::quoteIdentifier($tableName),
        );
    }
}
