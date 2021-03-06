from glue_migrator.transforms.generic_transforms import (
    transform_catalog_to_df,
    transform_items_to_item,
    transform_databases_tables_partitions,
)
from glue_migrator.migrators.exporter import Exporter
from glue_migrator.services.hive_metastore import HiveMetastore
from glue_migrator.services.connection_handler import ConnectionHandler
from glue_migrator.utils.logger import Logger
from glue_migrator.utils.schema_helper import change_schemas
from glue_migrator.utils.validators import validate_aws_regions

logger = Logger()
logger.basicConfig()


class JdbcExporter(Exporter):
    def __init__(self, glue_context, spark_context, sql_context):
        self.glue_context = glue_context
        self.spark_context = spark_context
        self.sql_context = sql_context
        self.CONNECTION_TYPE_NAME = (
            "com.amazonaws.services.glue.connections.DataCatalogConnection"
        )

    def read_databases(self, datacatalog_name, database_arr, region):
        databases = None
        tables = None
        partitions = None
        for database in database_arr:
            logger.info(f"Reading tables from database {database}")
            dyf = self.glue_context.create_dynamic_frame.from_options(
                connection_type=self.CONNECTION_TYPE_NAME,
                connection_options={
                    "catalog.name": datacatalog_name,
                    "catalog.database": database,
                    "catalog.region": region,
                },
            )

            logger.info(f"Transforming df tables from database {database}")
            df = transform_catalog_to_df(dyf)

            logger.info(f"Filtering database, tables and partitions records ")
            dc_databases_no_schema = df.where('type = "database"')
            dc_tables_no_schema = df.where('type = "table"')
            dc_partitions_no_schema = df.where('type = "partition"')

            logger.info(f"Applying schemas to dataframes")
            (dc_databases, dc_tables, dc_partitions) = change_schemas(
                self.sql_context,
                dc_databases_no_schema,
                dc_tables_no_schema,
                dc_partitions_no_schema,
            )
            (a_databases, a_tables, a_partitions) = transform_items_to_item(
                dc_databases=dc_databases,
                dc_tables=dc_tables,
                dc_partitions=dc_partitions,
            )
            databases = databases.union(a_databases) if databases else a_databases
            tables = tables.union(a_tables) if tables else a_tables
            partitions = partitions.union(a_partitions) if partitions else a_partitions

        return (databases, tables, partitions)

    def export_datacatalog(self, databases, tables, partitions, connection):
        hive_metastore = HiveMetastore(connection, self.sql_context)
        transform_databases_tables_partitions(
            self.spark_context,
            self.sql_context,
            hive_metastore,
            databases,
            tables,
            partitions,
        )
        hive_metastore.write_tables_to_metastore()

    def run(self, options={}):
        """
        Run the exporter pipeline migrating from Glue to external Hive database
        :param options: argparse object with command line parameters
        :return: export execution
        """
        logger.info(f"Running exporter process")
        validate_aws_regions(options["region"])

        database_arr = options["database_names"].split(";")

        (databases, tables, partitions) = self.read_databases(
            datacatalog_name="datacatalog",
            database_arr=database_arr,
            region=options.get("region", "us-east-1"),
        )

        connection = ConnectionHandler(
            glue_context=self.glue_context,
            connection_name=options.get("connection_name", None),
            file_path=options.get("path_connection_file", None),
        ).get_credentials()

        self.export_datacatalog(
            databases=databases,
            tables=tables,
            partitions=partitions,
            connection=connection,
        )
