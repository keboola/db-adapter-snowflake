<?php

declare(strict_types=1);

namespace Keboola\SnowflakeDbAdapter;

use Keboola\SnowflakeDbAdapter\Exception\SnowflakeDbAdapterException;
use Throwable;

class Connection
{
    /**
     * @var resource odbc handle
     */
    private $connection;

    private QueryBuilder $queryBuilder;

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
     * - clientSessionKeepAlive (bool) - Parameter that indicates whether to force a user
     * to log in again after a period of inactivity in the session
     *
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
            'schema',
            'warehouse',
            'runId',
            'clientSessionKeepAlive',
            'application',
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

        if (isset($options['schema'])) {
            $dsn .= ';Schema=' . $this->quoteIdentifier($options['schema']);
        }

        if (isset($options['warehouse'])) {
            $dsn .= ';Warehouse=' . QueryBuilder::quoteIdentifier($options['warehouse']);
        }

        if (isset($options['application'])) {
            $dsn .= ';application=' . QueryBuilder::quoteIdentifier($options['application']);
        }

        if (isset($options['clientSessionKeepAlive']) && $options['clientSessionKeepAlive']) {
            $dsn .= ';CLIENT_SESSION_KEEP_ALIVE=TRUE';
        }

        $attemptNumber = 0;
        do {
            if ($attemptNumber > 0) {
                sleep(pow(2, $attemptNumber));
            }
            try {
                $this->connection = odbc_connect($dsn, $options['user'], $options['password']);

                if (isset($options['runId'])) {
                    $queryTag = [
                        'runId' => $options['runId'],
                    ];
                    $this->query("ALTER SESSION SET QUERY_TAG='" . json_encode($queryTag) . "';");
                }
            } catch (Throwable $e) {
                // try again if it is a failed rest request
                if (stristr($e->getMessage(), 'S1000') !== false) {
                    $attemptNumber++;
                    if ($attemptNumber > $maxBackoffAttempts) {
                        throw new SnowflakeDbAdapterException(
                            'Initializing Snowflake connection failed: ' . $e->getMessage(),
                            0,
                            $e,
                        );
                    }
                } else {
                    throw new SnowflakeDbAdapterException(
                        'Initializing Snowflake connection failed: ' . $e->getMessage(),
                        0,
                        $e,
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
        } catch (Throwable $e) {
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
        } catch (Throwable $e) {
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
        } catch (Throwable $e) {
            (new ExceptionHandler())->handleException($e, $sql);
        }
    }

    /**
     * Avoid odbc file open http://php.net/manual/en/function.odbc-execute.php
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
}
