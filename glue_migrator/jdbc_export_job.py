from __future__ import print_function

from awsglue.context import GlueContext
from glue_migrator.schemas.datacatalog import DataCatalogSchemas
from glue_migrator.services.hive_metastore import HiveMetastore
from hive_metastore_migration import *

CONNECTION_TYPE_NAME = 'com.amazonaws.services.glue.connections.DataCatalogConnection'

def datacatalog_migrate_to_s3(databases, tables, partitions, output_path):

    # load
    databases.write.format('json').mode('overwrite').save(output_path + 'databases')
    tables.write.format('json').mode('overwrite').save(output_path + 'tables')
    partitions.write.format('json').mode('overwrite').save(output_path + 'partitions')


# apply hard-coded schema on dataframes, ensure schema is consistent for transformations
def change_schemas(sql_context, databases, tables, partitions):
    databases = sql_context.read.json(databases.toJSON(), schema=DataCatalogSchemas.DATACATALOG_DATABASE_SCHEMA)
    tables = sql_context.read.json(tables.toJSON(), schema=DataCatalogSchemas.DATACATALOG_TABLE_SCHEMA)
    partitions = sql_context.read.json(partitions.toJSON(), schema=DataCatalogSchemas.DATACATALOG_PARTITION_SCHEMA)

    return (databases, tables, partitions)


def main():
    to_s3 = 'to-s3'
    to_jdbc = 'to-jdbc'
    parser = argparse.ArgumentParser(prog=sys.argv[0])
    parser.add_argument('-m', '--mode', required=True, choices=[to_s3, to_jdbc], help='Choose to migrate from datacatalog to s3 or to metastore')
    parser.add_argument('--database-names', required=True, help='Semicolon-separated list of names of database in Datacatalog to export')
    parser.add_argument('-o', '--output-path', required=False, help='Output path, either local directory or S3 path')
    parser.add_argument('-c', '--connection-name', required=False, help='Glue Connection name for Hive metastore JDBC connection')
    parser.add_argument('-R', '--region', required=False, help='AWS region of source Glue DataCatalog, default to "us-east-1"')

    options = get_options(parser, sys.argv)
    if options['mode'] == to_s3:
        validate_options_in_mode(
            options=options, mode=to_s3,
            required_options=['output_path'],
            not_allowed_options=['connection_name']
        )
    elif options['mode'] == to_jdbc:
        validate_options_in_mode(
            options=options, mode=to_jdbc,
            required_options=['connection_name'],
            not_allowed_options=['output_path']
        )
    else:
        raise AssertionError('unknown mode ' + options['mode'])

    validate_aws_regions(options['region'])

    # spark env
    (conf, sc, sql_context) = get_spark_env()
    glue_context = GlueContext(sc)

    # extract from datacatalog reader
    database_arr = options['database_names'].split(';')

    (databases, tables, partitions) = read_databases_from_catalog(
        sql_context=sql_context,
        glue_context=glue_context,
        datacatalog_name='datacatalog',
        database_arr=database_arr,
        region=options.get('region') or 'us-east-1'
    )

    if options['mode'] == to_s3:
        output_path = get_output_dir(options['output_path'])
        datacatalog_migrate_to_s3(
            databases=databases,
            tables=tables,
            partitions=partitions,
            output_path=output_path
        )
    elif options['mode'] == to_jdbc:
        connection_name = options['connection_name']
        datacatalog_migrate_to_hive_metastore(
            sc=sc,
            sql_context=sql_context,
            databases=databases,
            tables=tables,
            partitions=partitions,
            connection=glue_context.extract_jdbc_conf(connection_name)
        )

if __name__ == '__main__':
    main()
