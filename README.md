# db-adapter-snowflake

[![Build Status](https://travis-ci.com/keboola/db-adapter-snowflake.svg?branch=master)](https://travis-ci.com/keboola/db-adapter-snowflake)

## Development
 
Clone this repository and init the workspace with following command:

```
git clone https://github.com/keboola/db-adapter-snowflake
cd db-adapter-snowflake
docker-compose build
docker-compose run --rm dev composer install --no-scripts
```

Run the test suite using this command:

```
docker-compose run --rm dev composer tests
```
