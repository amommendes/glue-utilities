# Glue Utilities

This repository is based on [hive metastore migration scripts](https://github.com/aws-samples/aws-glue-samples/tree/master/utilities/Hive_metastore_migration/src) from [aws-glue-samples](https://github.com/aws-samples/aws-glue-samples/).

> Note: By now, only migration from Glue to JDBC Postgres is available.

## Usage

You need first install glue spark version as described [here](https://github.com/awslabs/aws-glue-libs).
Glue spark will also provides `GlueContext` which is necessary in the migration process, because we need to connect in the Glue database.
> Apparently AWS does not provide any endpoint to connect in the Glue database, for example, using JDBC.

The lib authenticates in AWS glue based on AWS environment secrets variables:

```shell
export AWS_ACCESS_KEY_ID="MY_KEY"
export AWS_SECRET_ACCESS_KEY="MY_SECRET"
export AWS_SESSION_TOKEN="MY_TOKEN"
export AWS_REGION=us-east-1
```

After installation, you can clone this repo and run the desired job.

```shell
usage: /PATH/TO/PROJECT/glue-utilities/glue_migrator/export_job.py
       [-h] (-p PATH_CONNECTION_FILE | -c CONNECTION_NAME) -m {to-s3,to-jdbc}
       --database-names DATABASE_NAMES [-R REGION]

optional arguments:
  -h, --help            show this help message and exit
  -p PATH_CONNECTION_FILE, --path-connection-file PATH_CONNECTION_FILE
                        Path to file with database credentials data
  -c CONNECTION_NAME, --connection-name CONNECTION_NAME
                        Glue Connection name for Hive metastore JDBC
                        connection. You can only set a connection name
                         OR a file with credential data
  -m {to-s3,to-jdbc}, --mode {to-s3,to-jdbc}
                        Choose to migrate from datacatalog to s3 or to
                        metastore
  --database-names DATABASE_NAMES
                        Semicolon-separated list of names of database in
                        Datacatalog to export
  -R REGION, --region REGION
                        AWS region of source Glue DataCatalog, default to "us-
                        east-1"
```

It is possible to run jobs using [AWS Glue docker container](https://aws.amazon.com/blogs/big-data/developing-aws-glue-etl-jobs-locally-using-a-container/). 
You can define the AWS environment variables directly into your dockerfile or define them [when starting the container](https://docs.docker.com/engine/reference/commandline/run/).

First, build the egg python package:
```shell
python setup.py bdist_egg
```

Then run the container
> Change the volumes paths according to your directory structure, databases, job memory configuration and log path
```shell
nohup docker run  -d \
-v /PATH/TO/PROJECT/glue-utilities/glue_migrator/:/home/glue_migrator \
-v /PATH/TO/PROJECT/glue-utilities/logs:/home/glue_migrator/logs \
-v /PATH/TO/PROJECT/glue-utilities/credentials.json:/home/glue_migrator/resources/credentials.json \
-v /PATH/TO/PROJECT/glue-utilities/dist/:/home/glue_migrator/dist/ \
-e AWS_ACCESS_KEY_ID="KEY_ACCESS" \
-e AWS_SECRET_ACCESS_KEY="SECRET_KEY" \
-e AWS_SESSION_TOKEN="TOKEN" \
-e AWS_REGION=us-east-1 \
--workdir=/home \
amazon/aws-glue-libs:glue_libs_1.0.0_image_01 \
/home/aws-glue-libs/bin/gluesparksubmit --master local[*] \
--driver-memory 8g \
--executor-memory  16g \
--py-files /home/glue_migrator/dist/glue_migrator-0.0.1-py3.6.egg \
/home/glue_migrator/export_job.py \
--mode "to-jdbc" \
--database-names "db1;db2" \
-p /home/glue_migrator/resources/credentials.json > /tmp/glue_migrator-$(date +%d%m%Y-%H%M%S).log
```

## To-Dos
- [ ] Add Catalog Name in "DBS"
- [ ] Unit Tests
- [ ] Add drivers to other databases (MySQL, SQL Server, etc)
- [ ] Migrate all databases at once (probably listing all databases with boto3)
- [ ] Handle case sensitive databases, as Postgres
- [ ] Unit tests
