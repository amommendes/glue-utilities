from pyspark.sql.types import StringType, StructField, StructType, LongType, IntegerType
from glue_migrator.schemas.datacatalog import DataCatalogSchemas

def construct_struct_schema(schema_tuples_list):
    struct_fields = []
    atomic_types_dict = {
        'int': IntegerType(),
        'long': LongType(),
        'string': StringType()
    }
    for (col_name, col_type, nullable) in schema_tuples_list:
        field_type = atomic_types_dict[col_type]
        struct_fields.append(StructField(name=col_name, dataType=field_type, nullable=nullable))
    return StructType(struct_fields)

def change_schemas(sql_context, databases, tables, partitions):
    databases = sql_context.read.json(databases.toJSON(), schema=DataCatalogSchemas.DATACATALOG_DATABASE_SCHEMA)
    tables = sql_context.read.json(tables.toJSON(), schema=DataCatalogSchemas.DATACATALOG_TABLE_SCHEMA)
    partitions = sql_context.read.json(partitions.toJSON(), schema=DataCatalogSchemas.DATACATALOG_PARTITION_SCHEMA)
    return (databases, tables, partitions)

def rename_columns(df, rename_tuples=None):
    """
    Rename columns, for each key in rename_map, rename column from key to value
    :param df: dataframe
    :param rename_map: map for columns to be renamed
    :return: new dataframe with columns renamed
    """
    for old, new in rename_tuples:
        df = df.withColumnRenamed(old, new)
    return df

def get_schema_type(df, column_name):
    return df.select(column_name).schema.fields[0].dataType
