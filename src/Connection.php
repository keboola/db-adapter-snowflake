<?php

declare(strict_types=1);

namespace Keboola\SnowflakeDbAdapter;

use Keboola\SnowflakeDbAdapter\Exception\SnowflakeDbAdapterException;
use Keboola\SnowflakeDbAdapter\Connection\Expr;

class Connection
{
    public const OBJECT_TYPE_DATABASE = 'DATABASE';
    public const OBJECT_TYPE_ROLE = 'ROLE';
    public const OBJECT_TYPE_SCHEMA = 'SCHEMA';
    public const OBJECT_TYPE_TABLE = 'TABLE';
    public const OBJECT_TYPE_VIEW = 'VIEW';
    public const OBJECT_TYPE_STAGE = 'STAGE';
    public const OBJECT_TYPE_USER = 'USER';
    public const OBJECT_TYPE_WAREHOUSE = 'WAREHOUSE';

    /**
     * @var resource odbc handle
     */
    private $connection;

    /** @var QueryBuilder */
    private $queryBuilder;

    /**
     * The connection constructor accepts the following options:
     * - host (string, required) - hostname
     * - port (int, optional) - port - default 443
     * - user (string, required) - username
     * - password (string, required) - password
     * - warehouse (string) - default warehouse to use
     * - database (string) - default database to use
     * - tracing (int) - the level of detail to be logged in the driver trace files
     * - loginTimeout (int) - Specifies how long to wait for a response when
     * connecting to the Snowflake service before returning a login failure
     * error.
     * - networkTimeout (int) - Specifies how long to wait for a response when
     * interacting with the Snowflake service before returning an error. Zero
     * (0) indicates no network timeout is set.
     * - queryTimeout (int) - Specifies how long to wait for a query to complete
     * before returning an error. Zero (0) indicates to wait indefinitely.
     *
     * @param array $options
     */
    public function __construct(array $options)
    {
        $this->queryBuilder = new QueryBuilder();
        $requiredOptions = [
            'host',
            'user',
            'password',
        ];

        $allowedOptions = [
            'host',
            'user',
            'password',
            'port',
            'tracing',
            'loginTimeout',
            'networkTimeout',
            'queryTimeout',
            'maxBackoffAttempts',
            'database',
            'warehouse',
        ];

        $missingOptions = array_diff($requiredOptions, array_keys($options));
        if (!empty($missingOptions)) {
            throw new SnowflakeDbAdapterException('Missing options: ' . implode(', ', $missingOptions));
        }

        $unknownOptions = array_diff(array_keys($options), $allowedOptions);
        if (!empty($unknownOptions)) {
            throw new SnowflakeDbAdapterException('Unknown options: ' . implode(', ', $unknownOptions));
        }

        $port = isset($options['port']) ? (int) $options['port'] : 443;
        $tracing = isset($options['tracing']) ? (int) $options['tracing'] : 0;
        $maxBackoffAttempts = isset($options['maxBackoffAttempts']) ? (int) $options['maxBackoffAttempts'] : 5;

        $dsn = 'Driver=SnowflakeDSIIDriver;Server=' . $options['host'];
        $dsn .= ';Port=' . $port;
        $dsn .= ';Tracing=' . $tracing;

        if (isset($options['loginTimeout'])) {
            $dsn .= ';Login_timeout=' . (int) $options['loginTimeout'];
        }

        if (isset($options['networkTimeout'])) {
            $dsn .= ';Network_timeout=' . (int) $options['networkTimeout'];
        }

        if (isset($options['queryTimeout'])) {
            $dsn .= ';Query_timeout=' . (int) $options['queryTimeout'];
        }

        if (isset($options['database'])) {
            $dsn .= ';Database=' . QueryBuilder::quoteIdentifier($options['database']);
        }

        if (isset($options['warehouse'])) {
            $dsn .= ';Warehouse=' . QueryBuilder::quoteIdentifier($options['warehouse']);
        }

        $attemptNumber = 0;
        do {
            if ($attemptNumber > 0) {
                sleep(pow(2, $attemptNumber));
            }
            try {
                $this->connection = odbc_connect($dsn, $options['user'], $options['password']);
            } catch (\Throwable $e) {
                // try again if it is a failed rest request
                if (stristr($e->getMessage(), 'S1000') !== false) {
                    $attemptNumber++;
                    if ($attemptNumber > $maxBackoffAttempts) {
                        throw new SnowflakeDbAdapterException(
                            'Initializing Snowflake connection failed: ' . $e->getMessage(),
                            0,
                            $e
                        );
                    }
                } else {
                    throw new SnowflakeDbAdapterException(
                        'Initializing Snowflake connection failed: ' . $e->getMessage(),
                        0,
                        $e
                    );
                }
            }
        } while ($this->connection === null);
    }

    public function __destruct()
    {
        $this->disconnect();
    }

    /**
     * @deprecated use QueryBuilder::quoteIdentifier instead
     */
    // phpcs:ignore SlevomatCodingStandard.TypeHints.TypeHintDeclaration.UselessDocComment
    public function quoteIdentifier(string $value): string
    {
        return QueryBuilder::quoteIdentifier($value);
    }

    /**
     * Returns information about table:
     *  - name
     *  - bytes
     *  - rows
     */
    public function describeTable(string $schemaName, string $tableName): array
    {
        $tables = $this->fetchAll($this->queryBuilder->showTableInSchema($schemaName, $tableName));

        foreach ($tables as $table) {
            if ($table['name'] === $tableName) {
                return $table;
            }
        }

        throw new SnowflakeDbAdapterException("Table $tableName not found in schema $schemaName");
    }

    public function describeTableColumns(string $schemaName, string $tableName): array
    {
        return $this->fetchAll($this->queryBuilder->showColumns($schemaName, $tableName));
    }

    public function getTableColumns(string $schemaName, string $tableName): array
    {
        return array_map(function ($column) {
            return $column['column_name'];
        }, $this->describeTableColumns($schemaName, $tableName));
    }

    public function getTablePrimaryKey(string $schemaName, string $tableName): array
    {
        $cols = $this->fetchAll($this->queryBuilder->describeTable($schemaName, $tableName));
        $pkCols = [];
        foreach ($cols as $col) {
            if ($col['primary key'] !== 'Y') {
                continue;
            }
            $pkCols[] = $col['name'];
        }

        return $pkCols;
    }

    public function query(string $sql, array $bind = []): void
    {
        try {
            $stmt = odbc_prepare($this->connection, $sql);
            odbc_execute($stmt, $this->repairBinding($bind));
            odbc_free_result($stmt);
        } catch (\Throwable $e) {
            (new ExceptionHandler())->handleException($e, $sql);
        }
    }

    public function fetchAll(string $sql, array $bind = []): array
    {
        $rows = [];
        try {
            $stmt = odbc_prepare($this->connection, $sql);
            odbc_execute($stmt, $this->repairBinding($bind));
            while ($row = odbc_fetch_array($stmt)) {
                $rows[] = $row;
            }
            odbc_free_result($stmt);
        } catch (\Throwable $e) {
            (new ExceptionHandler())->handleException($e, $sql);
        }
        return $rows;
    }

    public function fetch(string $sql, array $bind, callable $callback): void
    {
        try {
            $stmt = odbc_prepare($this->connection, $sql);
            odbc_execute($stmt, $this->repairBinding($bind));
            while ($row = odbc_fetch_array($stmt)) {
                $callback($row);
            }
            odbc_free_result($stmt);
        } catch (\Throwable $e) {
            (new ExceptionHandler())->handleException($e, $sql);
        }
    }

    /**
     * Avoid odbc file open http://php.net/manual/en/function.odbc-execute.php
     * @param array $bind
     * @return array
     */
    private function repairBinding(array $bind): array
    {
        return array_map(function ($value) {
            if (preg_match("/^'.*'$/", $value)) {
                return " {$value} ";
            } else {
                return $value;
            }
        }, $bind);
    }

    public function disconnect(): void
    {
        if (is_resource($this->connection)) {
            odbc_close($this->connection);
        }
    }

    public function alterUser(string $userName, array $options): void
    {
        if (!count($options)) {
            throw new SnowflakeDbAdapterException('Nothing to alter without options');
        }

        $this->query(
            $this->queryBuilder->alterUser($userName, $options)
        );
    }

    public static function createQuotedOptionsStringFromArray(array $otherOptions): string
    {
        $otherOptionsString = '';
        foreach ($otherOptions as $option => $optionValue) {
            $quotedValue = $optionValue instanceof Expr ? $optionValue->getValue() : Connection::quote($optionValue);
            $otherOptionsString .= strtoupper($option) . '=' . $quotedValue . \PHP_EOL;
        }
        return $otherOptionsString;
    }

    public function createRole(string $roleName): void
    {
        $this->query(
            $this->queryBuilder->createRole($roleName)
        );
    }

    public function createSchema(string $schema): void
    {
        $this->query(
            $this->queryBuilder->createSchema($schema)
        );
    }

    public function createUser(string $userName, string $password, array $otherOptions): void
    {
        $this->query(
            $this->queryBuilder->createUser($userName, $password, $otherOptions)
        );
    }

    /**
     * @return mixed[]
     */
    public function describeUser(string $userName): array
    {
        $userFields = $this->fetchAll(
            $this->queryBuilder->describeUser($userName)
        );
        $result = [];
        foreach ($userFields as $userField) {
            $result[strtolower($userField['property'])] = $userField['value'];
        }
        return $result;
    }

    public function fetchRoles(?string $roleLike = null): array
    {
        return $this->fetchAll(
            $this->queryBuilder->showRoles($roleLike)
        );
    }

    public function fetchSchemasLike(string $schemaName): array
    {
        return $this->fetchAll(
            $this->queryBuilder->showSchemas($schemaName)
        );
    }

    public function grantOnDatabaseToRole(string $database, string $role, array $grants): void
    {
        $this->grantToObjectTypeOnObjectType(
            Connection::OBJECT_TYPE_DATABASE,
            $database,
            Connection::OBJECT_TYPE_ROLE,
            $role,
            $grants
        );
    }

    public function grantOnSchemaToRole(string $schemaName, string $role, array $grants): void
    {
        $this->grantToObjectTypeOnObjectType(
            Connection::OBJECT_TYPE_SCHEMA,
            $schemaName,
            Connection::OBJECT_TYPE_ROLE,
            $role,
            $grants
        );
    }

    public function grantOnWarehouseToRole(string $warehouse, string $role, array $grants): void
    {
        $this->grantToObjectTypeOnObjectType(
            Connection::OBJECT_TYPE_WAREHOUSE,
            $warehouse,
            Connection::OBJECT_TYPE_ROLE,
            $role,
            $grants
        );
    }

    private function grantRoleToObject(string $role, string $granteeName, string $objectType): void
    {
        $this->query(
            $this->queryBuilder->grantRoleToObjectType($role, $granteeName, $objectType)
        );
    }

    public function grantRoleToRole(string $roleToBeGranted, string $roleToGrantTo): void
    {
        $this->grantRoleToObject($roleToBeGranted, $roleToGrantTo, self::OBJECT_TYPE_ROLE);
    }

    public function grantRoleToUser(string $role, string $userName): void
    {
        $this->grantRoleToObject($role, $userName, self::OBJECT_TYPE_USER);
    }

    public function grantSelectOnAllTablesInSchemaToRole(string $schemaName, string $role): void
    {
        $this->query(
            $this->queryBuilder->grantSelectOnAllTablesInSchemaToRole($schemaName, $role)
        );
    }

    private function grantToObjectTypeOnObjectType(
        string $grantOnObjectType,
        string $grantOnName,
        string $granteeObjectType,
        string $grantToName,
        array $grant
    ): void {
        $this->query(
            $this->queryBuilder->grantToObjectTypeOnObjectType(
                $grantOnObjectType,
                $grantOnName,
                $granteeObjectType,
                $grantToName,
                $grant
            )
        );
    }

    private function grantToObjectTypeOnAllObjectTypesInSchema(
        string $grantOnObjectType,
        string $schemaName,
        string $granteeObjectType,
        string $grantToName,
        array $grant
    ): void {
        $this->query(
            $this->queryBuilder->grantToObjectTypeOnAllObjectTypesInSchema(
                $grantOnObjectType,
                $schemaName,
                $granteeObjectType,
                $grantToName,
                $grant
            )
        );
    }

    public function grantOnAllObjectTypesInSchemaToRole(
        string $grantOnObjectType,
        string $schemaName,
        string $roleName,
        array $grant
    ): void {
        $this->grantToObjectTypeOnAllObjectTypesInSchema(
            $grantOnObjectType,
            $schemaName,
            self::OBJECT_TYPE_ROLE,
            $roleName,
            $grant
        );
    }

    public static function quote(string $value): string
    {
        $q = "'";
        return ($q . str_replace("$q", "$q$q", $value) . $q);
    }

    private function revokeRoleFromObjectType(
        string $grantedRole,
        string $roleGrantedTo,
        string $objectTypeGrantedTo
    ): void {
        $this->query(
            $this->queryBuilder->revokeRoleFromObjectType($grantedRole, $roleGrantedTo, $objectTypeGrantedTo)
        );
    }

    public function revokeRoleGrantFromRole(string $grantedRole, string $roleGrantedTo): void
    {
        $this->revokeRoleFromObjectType($grantedRole, $roleGrantedTo, Connection::OBJECT_TYPE_ROLE);
    }

    /**
     * @return mixed[][]
     */
    public function showGrantsOfRole(string $role): array
    {
        return $this->fetchAll(
            $this->queryBuilder->showGrantsOfRole($role)
        );
    }

    /**
     * @return mixed[]
     */
    public function showGrantsToRole(string $role): array
    {
        return $this->fetchAll(
            $this->queryBuilder->showGrantsToRole($role)
        );
    }

    public function getCurrentRole(): string
    {
        $res = $this->fetchAll($this->queryBuilder->currentRole());
        return $res[0]['name'];
    }
}
