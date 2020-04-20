# db-adapter-snowflake

[![Build Status](https://travis-ci.com/keboola/db-adapter-snowflake.svg?branch=master)](https://travis-ci.com/keboola/db-adapter-snowflake)

## Breaking changes

### From `php-db-import`
* no longer throws `Keboola\Db\Import\Exception`, but `\Keboola\SnowflakeDbAdapter\Exception\SnowflakeDbAdapterException`
* rewritten `ExceptionHandler` does not throw `Keboola\Db\Import\Exception` but `\Keboola\SnowflakeDbAdapter\Exception\StringTooLongException` for too long string 
* renamed method `\Keboola\SnowflakeDbAdapter\ExceptionHandler::handleException`

### From `app-snowflake-dwh-manager`

* no longer throws `\Exception`, but `\Keboola\SnowflakeDbAdapter\Exception\SnowflakeDbAdapterException`

## Development
 
* Clone this repository and init the workspace with following command:

```
git clone https://github.com/keboola/db-adapter-snowflake
cd db-adapter-snowflake
docker-compose build
docker-compose run --rm dev composer install --no-scripts
```

* Create snowflake user (supply your own password and warehouse name):
```
CREATE DATABASE "TRAVIS_SNOWFLAKE_ADAPTER" DATA_RETENTION_TIME_IN_DAYS = 0;
CREATE ROLE "TRAVIS_SNOWFLAKE_ADAPTER";
GRANT OWNERSHIP ON DATABASE "TRAVIS_SNOWFLAKE_ADAPTER" TO ROLE "TRAVIS_SNOWFLAKE_ADAPTER";
CREATE USER "TRAVIS_SNOWFLAKE_ADAPTER"
    PASSWORD = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
    DEFAULT_ROLE = "TRAVIS_SNOWFLAKE_ADAPTER";
GRANT ROLE "TRAVIS_SNOWFLAKE_ADAPTER" TO USER "TRAVIS_SNOWFLAKE_ADAPTER";
GRANT ROLE "TRAVIS_SNOWFLAKE_ADAPTER" TO ROLE "ACCOUNTADMIN";
GRANT USAGE ON WAREHOUSE "DEV" TO ROLE "TRAVIS_SNOWFLAKE_ADAPTER";
GRANT USAGE ON DATABASE "TRAVIS_SNOWFLAKE_ADAPTER" TO ROLE "TRAVIS_SNOWFLAKE_ADAPTER";
CREATE SCHEMA "TRAVIS_SNOWFLAKE_ADAPTER"."TRAVIS_SNOWFLAKE_ADAPTER";
GRANT ALL ON SCHEMA "TRAVIS_SNOWFLAKE_ADAPTER"."TRAVIS_SNOWFLAKE_ADAPTER" TO ROLE "TRAVIS_SNOWFLAKE_ADAPTER";
```

* Fill credentials for created user in `.env` (required variables are in `.env.dist`). 
* Run the test suite using this command:

```
docker-compose run --rm dev composer tests
```
