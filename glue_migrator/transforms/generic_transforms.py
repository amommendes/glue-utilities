from pyspark.sql.functions import explode
from glue_migrator.transforms.datacatalog_transformer import DataCatalogTransformer


def transform_databases_tables_partitions(
    sc, sql_context, hive_metastore, databases, tables, partitions
):
    DataCatalogTransformer(sc, sql_context).transform(
        hms=hive_metastore, databases=databases, tables=tables, partitions=partitions
    )


def transform_items_to_item(dc_databases, dc_tables, dc_partitions):
    databases = dc_databases.select("*", explode("items").alias("item")).drop("items")
    tables = dc_tables.select("*", explode("items").alias("item")).drop("items")
    partitions = dc_partitions.select("*", explode("items").alias("item")).drop("items")

    return (databases, tables, partitions)


def transform_catalog_to_df(dyf):
    return dyf.toDF()
