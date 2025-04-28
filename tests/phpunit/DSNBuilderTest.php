<?php

declare(strict_types=1);

namespace Keboola\SnowflakeDbAdapter\Tests;

use Keboola\SnowflakeDbAdapter\Builder\DSNBuilder;
use Keboola\SnowflakeDbAdapter\Exception\PrivateKeyIsNotValid;
use PHPUnit\Framework\TestCase;

class DSNBuilderTest extends TestCase
{
    private const PRIVATE_PRIVATE_KEY = '-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCdA3EDPX6qduHB
bTejfJkmoIK2xpeWdsawZ3iEqhodFua7eGHWwZ8/qj82/WpweoHgCwQHEHg+CLxR
zyNufRd5ucsjoEFmB5bpQ98KV51Poa19Bfp6q5tpwFEdsam+SdRChyFX1SYB77cl
6ewuzZBXjWL1IW87RzNgJ0NQc5vIWhQNf+MpT0T2ZNaJQRE+jpg3tH5F/LMGbpq6
enaVAXCCfY4IK2tSXZri0VQlJwW1CYiBNdTL9ib5l292gSNMxEzloLXhMd2mO8zq
T6KJIWsRtf+oRD4ZaHFm6cLuNhx4Kp38rRKbmyMo9m9aDMzQyGsdbe+8qfZ3qXu+
w79icAjrAgMBAAECggEAI8mIhTYLfFwLWbSElXlNSP4VvQYnv+5DnCqBUC+bMx0o
qvslxbatDuxFa0m4bHmnx4KLQPbyiSUhkz30s/bEFoUP9YdN0K0TKwjCug89NkzC
B7iInlQl4KUqd+uqJkqanC1Dnsrg1lkmc/hM0mAdMa9yqi+cNDUm0xgv4hLvo2r1
C0XXSSwron0nHJHFmVz/gTPrIDRQYhHR65Gh3E1tgENVNO6qaYW1fVqZbzp6P5TY
+mYXZGnTgyPBlGVd3UvQDiawU8w7koLD+TDwg3PPqDaAsp1NIeXXwecjwN8fMyR6
6dCAjXv2giunn6R6F7RA5xWlfvFQEQsxOzpfdfNPQQKBgQDVd4gt4FFwLo90AkdB
KmOT2U4IGmChqbKVPkjnDxQ3uGbHBxnwfp7GFepcNm0qVOQt7KErp1/0RgyWgtKY
hqDTgt/8XH2rtNokmpeF30oha/0OM7FRMBRrBrM5xYk6MOpNOYmfJYlK4EyLU775
0H0jdDDhhAbDOD9KUegyiNpcqwKBgQC8TFgrNkTYmYNRRiLLrjh0oNZZuBMHv/F7
3Ba7cMpbl/RSYAG/7LR4mLK31s2Bki/8qwfH3xRaSnbX9IxWrhUOFbrqTVjMyjCB
d/yRBjWqSd5BGkGcDyTQvyAd0quHXWhdv3SKxx2KritusThlySEViybaD1rrkfbo
uhR6yhiEwQKBgHy+f2wfvDeMXfRzKGSietJ5mKoPkAyo+F1SqpOsMiplrln8gmIR
/ILRZ8U/YQft1/ImaAD0rJQ5Iz4JcTwE4JL51h7Jhf8Djr75QDbRR6bETnswJhzF
tgjdP3sxPoIs957tUskXzGVfMhvxcpbWCWrgiXggCTun8QRjXQe7BbBFAoGBALjm
8xGD2fkkTfoqOBLwgsmsArDZe/55MoWhKsVnTZ7ByY52bmFr7BAcSisSiHz685pW
zG4tlgvP4YHQx1p62XwaRJC94TxAM39/Nommol4U6WXehZzclhdSBxSiAgQL6mdc
kPGR82VyAH5TEoGJDq7cFQu+VlbK373KtD+bYpZBAoGAatuPpzN0XgepafhEgP9Y
6wPB1gQuMAEjP488ZNC2lKCapeZl+sxM1yRWo97UNhPIvmXaKuOmp/WPlL/29awd
HzF6xDBlsWxye/v72om4eMgDyG5G+ItErp5qcd55jx8zm6W+7GrZwpaL3fyzg9sk
FFdzKwQdMPWGCJTt0KdMw2s=
-----END PRIVATE KEY-----';

    public function testBuildDSNWithRole(): void
    {
        $dsn = DSNBuilder::build([
            'host' => 'host',
            'port' => '443',
            'database' => 'db',
            'warehouse' => 'whh',
            'user' => 'user',
            'password' => 'password',
            'roleName' => 'role',
        ]);

        self::assertEquals(
            'Driver=SnowflakeDSIIDriver;Server=host;Port=443;Tracing=0;Database="db";Warehouse="whh";Role="role"',
            $dsn,
        );
    }

    public function testBuildDSNWithKeyPair(): void
    {
        $dsn = DSNBuilder::build([
            'host' => 'host',
            'port' => '443',
            'database' => 'database',
            'warehouse' => 'warehouse',
            'user' => 'user',
            'password' => 'password',
            'roleName' => 'role',
            'privateKey' => self::PRIVATE_PRIVATE_KEY,
        ]);

        /** @codingStandardsIgnoreStart */
        self::assertTrue(
            @preg_match(
                '/^Driver=SnowflakeDSIIDriver;Server=host;Port=443;Tracing=0;Database="database";Warehouse="warehouse";Role="role";AUTHENTICATOR=SNOWFLAKE_JWT;PRIV_KEY_FILE=\/tmp\/snowflake_private_key_([a-zA-Z0-9]*).p8;UID=user$/',
                $dsn,
            ) === 1,
        );
        /** @codingStandardsIgnoreEnd */
    }

    public function testInvalidKeyPair(): void
    {
        self::expectException(PrivateKeyIsNotValid::class);

        DSNBuilder::build([
            'host' => 'host',
            'port' => '443',
            'database' => 'database',
            'warehouse' => 'warehouse',
            'user' => 'user',
            'password' => 'password',
            'roleName' => 'role',
            'privateKey' => 'totally bad key pair',
        ]);
    }
}
