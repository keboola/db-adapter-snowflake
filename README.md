# db-adapter-snowflake

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
CREATE DATABASE "GHA_SNOWFLAKE_ADAPTER" DATA_RETENTION_TIME_IN_DAYS = 0;
CREATE ROLE "GHA_SNOWFLAKE_ADAPTER";
GRANT OWNERSHIP ON DATABASE "GHA_SNOWFLAKE_ADAPTER" TO ROLE "GHA_SNOWFLAKE_ADAPTER";
CREATE USER "GHA_SNOWFLAKE_ADAPTER"
    PASSWORD = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
    DEFAULT_ROLE = "GHA_SNOWFLAKE_ADAPTER";
GRANT ROLE "GHA_SNOWFLAKE_ADAPTER" TO USER "GHA_SNOWFLAKE_ADAPTER";
GRANT ROLE "GHA_SNOWFLAKE_ADAPTER" TO ROLE "ACCOUNTADMIN";
GRANT USAGE ON WAREHOUSE "DEV" TO ROLE "GHA_SNOWFLAKE_ADAPTER";
GRANT USAGE ON DATABASE "GHA_SNOWFLAKE_ADAPTER" TO ROLE "GHA_SNOWFLAKE_ADAPTER";
CREATE SCHEMA "GHA_SNOWFLAKE_ADAPTER"."GHA_SNOWFLAKE_ADAPTER";
GRANT ALL ON SCHEMA "GHA_SNOWFLAKE_ADAPTER"."GHA_SNOWFLAKE_ADAPTER" TO ROLE "GHA_SNOWFLAKE_ADAPTER";
```

* Fill credentials for created user in `.env` (required variables are in `.env.dist`). 
* Run the test suite using this command:

```
docker-compose run --rm dev composer tests
```
