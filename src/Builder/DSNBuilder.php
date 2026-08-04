<?php

declare(strict_types=1);

namespace Keboola\SnowflakeDbAdapter\Builder;

use Keboola\SnowflakeDbAdapter\Exception\PrivateKeyIsNotValid;
use Keboola\SnowflakeDbAdapter\Exception\SnowflakeDbAdapterException;
use Keboola\SnowflakeDbAdapter\QueryBuilder;

class DSNBuilder
{
    /**
     * @param array<string, string> $options
     */
    public static function build(array $options): string
    {
        $requiredOptions = [
            'host',
            'user',
            'privateKey',
        ];

        $allowedOptions = [
            'host',
            'user',
            'privateKey',
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
            'roleName',
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
            $dsn .= ';Schema=' . QueryBuilder::quoteIdentifier($options['schema']);
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

        if (isset($options['roleName'])) {
            $dsn .= ';Role=' . QueryBuilder::quoteIdentifier($options['roleName']);
        }

        $dsn .= ';AUTHENTICATOR=SNOWFLAKE_JWT';
        $dsn .= ';PRIV_KEY_FILE=' . self::getPrivateKeyPath($options['privateKey']);
        $dsn .= ';UID=' . $options['user'];

        return $dsn;
    }

    private static function getPrivateKeyPath(string $privateKey): string
    {
        $privateKeyResource = openssl_pkey_get_private($privateKey);
        if (!$privateKeyResource) {
            throw new PrivateKeyIsNotValid();
        }

        $pemPKCS8 = '';
        openssl_pkey_export($privateKeyResource, $pemPKCS8);

        $privateKeyPath = tempnam(sys_get_temp_dir(), 'snowflake_private_key_' . uniqid()) . '.p8';
        file_put_contents($privateKeyPath, $pemPKCS8);

        return $privateKeyPath;
    }
}
