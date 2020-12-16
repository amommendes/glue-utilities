from enum import Enum
from glue_migrator.schemas.datacatalog import DataCatalogSchemas
from pyspark.sql.types import StringType, StructField, StructType


class MetastoreSchemas(Enum):

    METASTORE_PARTITION_SCHEMA = StructType(
        [
            StructField("database", StringType(), False),
            StructField("table", StringType(), False),
            StructField(
                "item", DataCatalogSchemas.DATACATALOG_PARTITION_ITEM_SCHEMA, True
            ),
            StructField("type", StringType(), False),
        ]
    )

    METASTORE_DATABASE_SCHEMA = StructType(
        [
            StructField(
                "item", DataCatalogSchemas.DATACATALOG_DATABASE_ITEM_SCHEMA, True
            ),
            StructField("type", StringType(), False),
        ]
    )

    METASTORE_TABLE_SCHEMA = StructType(
        [
            StructField("database", StringType(), False),
            StructField("type", StringType(), False),
            StructField("item", DataCatalogSchemas.DATACATALOG_TABLE_ITEM_SCHEMA, True),
        ]
    )
