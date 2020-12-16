from __future__ import print_function

from awsglue.context import GlueContext
from glue_migrator.utils.helpers import get_options
from glue_migrator.services.spark_service import SparkService
from glue_migrator.migrators.jdbc_exporter import JdbcExporter
from glue_migrator.schemas.datacatalog import DataCatalogSchemas
import sys
import argparse
from glue_migrator.utils.logger import Logger

logger = Logger()
logger.basicConfig()


def datacatalog_migrate_to_s3(databases, tables, partitions, output_path):

    # load
    databases.write.format("json").mode("overwrite").save(output_path + "databases")
    tables.write.format("json").mode("overwrite").save(output_path + "tables")
    partitions.write.format("json").mode("overwrite").save(output_path + "partitions")


# apply hard-coded schema on dataframes, ensure schema is consistent for transformations
def change_schemas(sql_context, databases, tables, partitions):
    databases = sql_context.read.json(
        databases.toJSON(), schema=DataCatalogSchemas.DATACATALOG_DATABASE_SCHEMA
    )
    tables = sql_context.read.json(
        tables.toJSON(), schema=DataCatalogSchemas.DATACATALOG_TABLE_SCHEMA
    )
    partitions = sql_context.read.json(
        partitions.toJSON(), schema=DataCatalogSchemas.DATACATALOG_PARTITION_SCHEMA
    )

    return (databases, tables, partitions)


def main():
    to_s3, to_jdbc = ("to-s3", "to-jdbc")
    parser = argparse.ArgumentParser(prog=sys.argv[0])

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "-p", "--path-connection-file", help="Path to file with connection data"
    )
    group.add_argument(
        "-c",
        "--connection-name",
        help="Glue Connection name for Hive metastore JDBC connection",
    )

    parser.add_argument(
        "-m",
        "--mode",
        required=True,
        choices=[to_s3, to_jdbc],
        help="Choose to migrate from datacatalog to s3 or to metastore",
    )
    parser.add_argument(
        "--database-names",
        required=True,
        help="Semicolon-separated list of names of database in Datacatalog to export",
    )
    parser.add_argument(
        "-R",
        "--region",
        required=False,
        help='AWS region of source Glue DataCatalog, default to "us-east-1"',
    )

    options = get_options(parser, sys.argv)
    print(options)

    # spark env
    spark = SparkService()
    (conf, sc, sql_context) = spark.get_spark_env()
    logger.info("Creating Glue Context")
    glue_context = GlueContext(sc)

    exporter = JdbcExporter(glue_context, sc, sql_context)
    exporter.run(options)


if __name__ == "__main__":
    main()
