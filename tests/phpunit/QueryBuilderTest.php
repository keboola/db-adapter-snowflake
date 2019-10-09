<?php

declare(strict_types=1);

namespace Keboola\SnowflakeDbAdapter\Tests;

use Keboola\SnowflakeDbAdapter\Connection;
use Keboola\SnowflakeDbAdapter\QueryBuilder;
use PHPUnit\Framework\TestCase;

class QueryBuilderTest extends TestCase
{
    /** @var QueryBuilder */
    private $queryBuilder;

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

    public function testGrantToObjectTypeOnAllObjectTypesInSchema(): void
    {
        $expected = 'GRANT SELECT,CREATE 
            ON ALL TABLES 
            IN SCHEMA "schema""Name"
            TO ROLE "role""Name"';
        $this->assertSame(
            $expected,
            $this->queryBuilder->grantToObjectTypeOnAllObjectTypesInSchema(
                Connection::OBJECT_TYPE_TABLE,
                'schema"Name',
                Connection::OBJECT_TYPE_ROLE,
                'role"Name',
                ['SELECT', 'CREATE']
            )
        );
    }

    public function testShowRoles(): void
    {
        $expected = 'SHOW ROLES LIKE \'role\\"Name\'';
        $this->assertSame(
            $expected,
            $this->queryBuilder->showRoles('role"Name')
        );
    }

    public function testCreateQuotedOptionsStringFromArray(): void
    {
        $expected = "X='some'
Y='options'
Z='that\\\"need'
Q='esc\\'aping'
";
        $this->assertSame(
            $expected,
            $this->queryBuilder::createQuotedOptionsStringFromArray([
                'x' => 'some',
                'y' => 'options',
                'z' => 'that"need',
                'q' => 'esc\'aping',
            ])
        );
    }

    public function testGrantToObjectTypeOnObjectType(): void
    {
        $expected = 'GRANT ALL 
            ON TABLE "table""na\'me"
            TO ROLE "role""na\'me"';
        $this->assertSame(
            $expected,
            $this->queryBuilder->grantToObjectTypeOnObjectType(
                Connection::OBJECT_TYPE_TABLE,
                'table"na\'me',
                Connection::OBJECT_TYPE_ROLE,
                'role"na\'me',
                ['ALL']
            )
        );
    }

    public function testGrantRoleToObjectType(): void
    {
        $expected = 'GRANT ROLE "role""na\'me" TO ROLE "role2""na\'me"';
        $this->assertSame(
            $expected,
            $this->queryBuilder->grantRoleToObjectType(
                'role"na\'me',
                'role2"na\'me',
                Connection::OBJECT_TYPE_ROLE
            )
        );
    }

    public function testRevokeRoleFromObjectType(): void
    {
        $expected = 'REVOKE ROLE "role""na\'me"
            FROM USER "user""na\'me"';
        $this->assertSame(
            $expected,
            $this->queryBuilder->revokeRoleFromObjectType(
                'role"na\'me',
                'user"na\'me',
                Connection::OBJECT_TYPE_USER
            )
        );
    }

    public function testCreateRole(): void
    {
        $expected = 'CREATE ROLE IF NOT EXISTS "role""na\'me"';
        $this->assertSame(
            $expected,
            $this->queryBuilder->createRole('role"na\'me')
        );
    }

    public function testCreateUser(): void
    {
        $expected = 'CREATE USER IF NOT EXISTS 
            "user""na\'me"
            PASSWORD = \'l33tPa$$worD\'
            LOGIN_NAME=\'franta.vomacka@keboola.com\'
DISPLAY_NAME=\'Franta Vomacka\'
';
        $this->assertSame(
            $expected,
            $this->queryBuilder->createUser(
                'user"na\'me',
                'l33tPa$$worD',
                [
                    'login_name' => 'franta.vomacka@keboola.com',
                    'display_name' => 'Franta Vomacka',
                ]
            )
        );
    }

    public function testShowGrantsOfRole(): void
    {
        $expected = 'SHOW GRANTS OF ROLE "role""na\'me"';
        $this->assertSame(
            $expected,
            $this->queryBuilder->showGrantsOfRole('role"na\'me')
        );
    }

    public function testCreateSchema(): void
    {
        $expected = 'CREATE SCHEMA IF NOT EXISTS "schema""na\'me"';
        $this->assertSame(
            $expected,
            $this->queryBuilder->createSchema('schema"na\'me')
        );
    }

    public function testDescribeUser(): void
    {
        $expected = 'DESCRIBE USER "user""na\'me"';
        $this->assertSame(
            $expected,
            $this->queryBuilder->describeUser('user"na\'me')
        );
    }

    public function testAlterUser(): void
    {
        $expected = 'ALTER USER IF EXISTS 
            "user""na\'me"
            SET 
            LOGIN_NAME=\'franta.vomacka@keboola.com\'
DISPLAY_NAME=\'Franta Vomacka\'
';
        $this->assertSame(
            $expected,
            $this->queryBuilder->alterUser(
                'user"na\'me',
                [
                    'login_name' => 'franta.vomacka@keboola.com',
                    'display_name' => 'Franta Vomacka',
                ]
            )
        );
    }

    public function testShowSchemas(): void
    {
        $expected = 'SHOW SCHEMAS LIKE \'schema\\"na\\\'me\'';
        $this->assertSame(
            $expected,
            $this->queryBuilder->showSchemas('schema"na\'me')
        );
    }

    public function testShowGrantsToRole(): void
    {
        $expected = 'SHOW GRANTS TO ROLE "role""na\'me"';
        $this->assertSame(
            $expected,
            $this->queryBuilder->showGrantsToRole('role"na\'me')
        );
    }

    public function testCurrentRole(): void
    {
        $expected = 'SELECT CURRENT_ROLE() AS "name"';
        $this->assertSame(
            $expected,
            $this->queryBuilder->currentRole()
        );
    }

    public function testGrantSelectOnAllTablesInSchemaToRole(): void
    {
        $expected = 'GRANT SELECT ON ALL TABLES IN SCHEMA "schema""na\'me" TO ROLE "role""na\'me"';
        $this->assertSame(
            $expected,
            $this->queryBuilder->grantSelectOnAllTablesInSchemaToRole(
                'schema"na\'me',
                'role"na\'me'
            )
        );
    }
}
