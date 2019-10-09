<?php

declare(strict_types=1);

namespace Keboola\SnowflakeDbAdapter;

use Keboola\SnowflakeDbAdapter\Connection\Expr;

class QueryBuilder
{
    /**
     * @param string|Expr $value
     */
    public static function quote($value): string
    {
        if ($value instanceof Expr) {
            return $value->getValue();
        }
        $q = "'";
        return $q . addslashes($value) . $q;
    }

    /**
     * @param string|Expr $value
     */
    public static function quoteIdentifier($value): string
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
            self::quoteIdentifier($schemaName)
        );
    }

    public function showColumns(string $schemaName, string $tableName): string
    {
        return sprintf(
            'SHOW COLUMNS IN %s.%s',
            self::quoteIdentifier($schemaName),
            self::quoteIdentifier($tableName)
        );
    }

    public function describeTable(string $schemaName, string $tableName): string
    {
        return sprintf(
            'DESC TABLE %s.%s',
            self::quoteIdentifier($schemaName),
            self::quoteIdentifier($tableName)
        );
    }

    public function alterUser(string $userName, array $options): string
    {
        return vsprintf(
            'ALTER USER IF EXISTS 
            %s
            SET 
            ' . Connection::createQuotedOptionsStringFromArray($options),
            [
                self::quoteIdentifier($userName),
            ]
        );
    }

    public function createRole(string $roleName): string
    {
        return vsprintf(
            'CREATE ROLE IF NOT EXISTS %s',
            [
                self::quoteIdentifier($roleName),
            ]
        );
    }

    public function createSchema(string $schema): string
    {
        return vsprintf(
            'CREATE SCHEMA IF NOT EXISTS %s',
            [
                self::quoteIdentifier($schema),
            ]
        );
    }

    public function createUser(string $userName, string $password, array $otherOptions): string
    {
        $otherOptionsString = Connection::createQuotedOptionsStringFromArray($otherOptions);
        return vsprintf(
            'CREATE USER IF NOT EXISTS 
            %s
            PASSWORD = %s
            ' . $otherOptionsString,
            [
                self::quoteIdentifier($userName),
                self::quote($password),
            ]
        );
    }

    public function describeUser(string $userName): string
    {
        return vsprintf(
            'DESCRIBE USER %s',
            [
                self::quoteIdentifier($userName),
            ]
        );
    }

    public function showRoles(?string $roleLike): string
    {
        $sql = 'SHOW ROLES';
        $args = [];
        if ($roleLike !== null) {
            $sql .= ' LIKE %s';
            $args[] = self::quote($roleLike);
        }

        $query = vsprintf(
            $sql,
            $args
        );
        return $query;
    }

    public function showSchemas(string $schemaName): string
    {
        return vsprintf(
            'SHOW SCHEMAS LIKE %s',
            [
                self::quote($schemaName),
            ]
        );
    }

    public function grantToObjectTypeOnObjectType(
        string $grantOnObjectType,
        string $grantOnName,
        string $granteeObjectType,
        string $grantToName,
        array $grant
    ): string {
        return vsprintf(
            'GRANT ' . implode(',', $grant) . ' 
            ON ' . $grantOnObjectType . ' %s
            TO ' . $granteeObjectType . ' %s',
            [
                self::quoteIdentifier($grantOnName),
                self::quoteIdentifier($grantToName),
            ]
        );
    }

    public function grantRoleToObjectType(string $role, string $granteeName, string $objectType): string
    {
        return vsprintf(
            'GRANT ROLE %s TO ' . $objectType . ' %s',
            [
                self::quoteIdentifier($role),
                self::quoteIdentifier($granteeName),
            ]
        );
    }

    public function grantSelectOnAllTablesInSchemaToRole(string $schemaName, string $role): string
    {
        return vsprintf(
            'GRANT SELECT ON ALL TABLES IN SCHEMA %s TO ROLE %s',
            [
                self::quoteIdentifier($schemaName),
                self::quoteIdentifier($role),
            ]
        );
    }

    public function grantToObjectTypeOnAllObjectTypesInSchema(
        string $grantOnObjectType,
        string $schemaName,
        string $granteeObjectType,
        string $grantToName,
        array $grant
    ): string {
        return vsprintf(
            'GRANT ' . implode(',', $grant) . ' 
            ON ALL ' . $grantOnObjectType . 'S 
            IN SCHEMA %s
            TO ' . $granteeObjectType . ' %s',
            [
                self::quoteIdentifier($schemaName),
                self::quoteIdentifier($grantToName),
            ]
        );
    }

    public function revokeRoleFromObjectType(
        string $grantedRole,
        string $roleGrantedTo,
        string $objectTypeGrantedTo
    ): string {
        return vsprintf(
            'REVOKE ROLE %s
            FROM ' . $objectTypeGrantedTo . ' %s',
            [
                self::quoteIdentifier($grantedRole),
                self::quoteIdentifier($roleGrantedTo),
            ]
        );
    }

    public function showGrantsOfRole(string $role): string
    {
        return vsprintf(
            'SHOW GRANTS OF ROLE %s',
            [
                self::quoteIdentifier($role),
            ]
        );
    }

    public function showGrantsToRole(string $role): string
    {
        return vsprintf(
            'SHOW GRANTS TO ROLE %s',
            [
                self::quoteIdentifier($role),
            ]
        );
    }

    public function currentRole(): string
    {
        return 'SELECT CURRENT_ROLE() AS "name"';
    }
}
