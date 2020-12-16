# Glue Utilities

This repository is based on [aws-glue-samples](https://github.com/aws-samples/aws-glue-samples/tree/master/utilities/Hive_metastore_migration/src).
By now, only migration from Glue to JDBC is available.

## Usage

You need first install glue spark version as described [here](https://github.com/awslabs/aws-glue-libs).
This will provides `GlueContext` which is necessary in the migration process.

The lib authenticates in AWS glue based on AWS environment variables:

```shell
export AWS_ACCESS_KEY_ID="MY_KEY"
export AWS_SECRET_ACCESS_KEY="MY_SECRET"
export AWS_SESSION_TOKEN="MY_TOKEN"
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
