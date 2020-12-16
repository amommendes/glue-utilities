from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    LongType,
    ArrayType,
    MapType,
    IntegerType,
    FloatType,
    BooleanType,
)


class DataCatalogSchemas:
    DATACATALOG_STORAGE_DESCRIPTOR_SCHEMA = StructType(
        [
            StructField("inputFormat", StringType(), True),
            StructField("compressed", BooleanType(), False),
            StructField("storedAsSubDirectories", BooleanType(), False),
            StructField("location", StringType(), True),
            StructField("numberOfBuckets", IntegerType(), False),
            StructField("outputFormat", StringType(), True),
            StructField("bucketColumns", ArrayType(StringType(), True), True),
            StructField(
                "columns",
                ArrayType(
                    StructType(
                        [
                            StructField("name", StringType(), True),
                            StructField("type", StringType(), True),
                            StructField("comment", StringType(), True),
                        ]
                    ),
                    True,
                ),
                True,
            ),
            StructField("parameters", MapType(StringType(), StringType(), True), True),
            StructField(
                "serdeInfo",
                StructType(
                    [
                        StructField("name", StringType(), True),
                        StructField("serializationLibrary", StringType(), True),
                        StructField(
                            "parameters",
                            MapType(StringType(), StringType(), True),
                            True,
                        ),
                    ]
                ),
                True,
            ),
            StructField(
                "skewedInfo",
                StructType(
                    [
                        StructField(
                            "skewedColumnNames", ArrayType(StringType(), True), True
                        ),
                        StructField(
                            "skewedColumnValueLocationMaps",
                            MapType(StringType(), StringType(), True),
                            True,
                        ),
                        StructField(
                            "skewedColumnValues", ArrayType(StringType(), True), True
                        ),
                    ]
                ),
                True,
            ),
            StructField(
                "sortColumns",
                ArrayType(
                    StructType(
                        [
                            StructField("column", StringType(), True),
                            StructField("order", IntegerType(), True),
                        ]
                    ),
                    True,
                ),
                True,
            ),
        ]
    )

    DATACATALOG_DATABASE_ITEM_SCHEMA = StructType(
        [
            StructField("description", StringType(), True),
            StructField("locationUri", StringType(), True),
            StructField("name", StringType(), False),
            StructField("parameters", MapType(StringType(), StringType(), True), True),
        ]
    )

    DATACATALOG_TABLE_ITEM_SCHEMA = StructType(
        [
            StructField("createTime", StringType(), True),
            StructField("lastAccessTime", StringType(), True),
            StructField("owner", StringType(), True),
            StructField("retention", IntegerType(), True),
            StructField("name", StringType(), False),
            StructField("tableType", StringType(), True),
            StructField("viewExpandedText", StringType(), True),
            StructField("viewOriginalText", StringType(), True),
            StructField("parameters", MapType(StringType(), StringType(), True), True),
            StructField(
                "partitionKeys",
                ArrayType(
                    StructType(
                        [
                            StructField("name", StringType(), True),
                            StructField("type", StringType(), True),
                            StructField("comment", StringType(), True),
                        ]
                    ),
                    True,
                ),
                True,
            ),
            StructField(
                "storageDescriptor", DATACATALOG_STORAGE_DESCRIPTOR_SCHEMA, True
            ),
        ]
    )

    DATACATALOG_PARTITION_ITEM_SCHEMA = StructType(
        [
            StructField("creationTime", StringType(), True),
            StructField("lastAccessTime", StringType(), True),
            StructField("namespaceName", StringType(), True),
            StructField("tableName", StringType(), True),
            StructField("parameters", MapType(StringType(), StringType(), True), True),
            StructField(
                "storageDescriptor", DATACATALOG_STORAGE_DESCRIPTOR_SCHEMA, True
            ),
            StructField("values", ArrayType(StringType(), False), False),
        ]
    )

    DATACATALOG_DATABASE_SCHEMA = StructType(
        [
            StructField(
                "items", ArrayType(DATACATALOG_DATABASE_ITEM_SCHEMA, False), True
            ),
            StructField("type", StringType(), False),
        ]
    )

    DATACATALOG_TABLE_SCHEMA = StructType(
        [
            StructField("database", StringType(), False),
            StructField("type", StringType(), False),
            StructField("items", ArrayType(DATACATALOG_TABLE_ITEM_SCHEMA, False), True),
        ]
    )

    DATACATALOG_PARTITION_SCHEMA = StructType(
        [
            StructField("database", StringType(), False),
            StructField("table", StringType(), False),
            StructField(
                "items", ArrayType(DATACATALOG_PARTITION_ITEM_SCHEMA, False), True
            ),
            StructField("type", StringType(), False),
        ]
    )
