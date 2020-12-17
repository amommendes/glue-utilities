import re
import time

from pyspark.sql.functions import col, concat, explode, lit, UserDefinedFunction
from pyspark.sql import Row
from pyspark.sql.types import (
    ArrayType,
    LongType,
    IntegerType,
    MapType,
    StructField,
    StringType,
    StructType,
)

from glue_migrator.utils.logger import Logger
from glue_migrator.utils.schema_helper import rename_columns

logger = Logger()
logger.basicConfig()


class DataCatalogTransformer:
    """
    Extracts data from DataCatalog entities into Hive metastore tables.
    """

    def __init__(self, sc, sql_context):
        self.sc = sc
        self.sql_context = sql_context
        self.start_id_map = dict()

    @staticmethod
    def udf_array_to_map(array):
        if array is None:
            return array
        return dict((i, v) for i, v in enumerate(array))

    @staticmethod
    def udf_partition_name_from_keys_vals(keys, vals):
        """
        udf_partition_name_from_keys_vals, create name string from array of keys and vals
        :param keys: array of partition keys from a datacatalog table
        :param vals: array of partition vals from a datacatalog partition
        :return: partition name, a string in the form 'key1(type),key2(type)=val1,val2'
        """
        if not keys or not vals:
            return ""
        partitions = []
        for key, val in zip(keys, vals):
            partitions.append(key.name + "=" + str(val))
        return "/".join(partitions)

    @staticmethod
    def udf_milliseconds_str_to_timestamp(milliseconds_str):
        if milliseconds_str is None:
            return 0
        else:
            try:
                return int(milliseconds_str)
            except TypeError as error:
                logger.error(f"Error while handling timestamp int: {error}. Returning now()")
                return int(time.time())


    @staticmethod
    def udf_string_list_str_to_list(string_list):
        """
        udf_string_list_str_to_list, transform string of a specific format into an array
        :param string_list: array represented as a string, format should be '<len>%['ele1', 'ele2', 'ele3']'
        :return: array, in this case would be [ele1, ele2, ele3]
        """
        try:
            r = re.compile("\d%\[('\w+',?\s?)+\]")
            if r.match(string_list) is None:
                return []
            return [
                item.strip()[1:-1]
                for item in string_list.split("%")[1][1:-1].split(",")
            ]
        except (IndexError, AssertionError):
            return []

    @staticmethod
    def udf_parameters_to_map(parameters):
        return parameters.asDict()

    @staticmethod
    def udf_with_non_null_locationuri(locationUri):
        if locationUri is None:
            return ""
        return locationUri

    @staticmethod
    def generate_idx_for_df(df, id_name, col_name, col_schema):
        """
        generate_idx_for_df, explodes rows with array as a column into a new row for each element in
        the array, with 'INTEGER_IDX' indicating its index in the original array.
        :param df: dataframe with array columns
        :param id_name: the id field of df
        :param col_name: the col of df to explode
        :param col_schema: the schema of each element in col_name array
        :return: new df with exploded rows.
        """
        idx_udf = UserDefinedFunction(
            DataCatalogTransformer.udf_array_to_map,
            MapType(IntegerType(), col_schema, True),
        )

        return df.withColumn("idx_columns", idx_udf(col(col_name))).select(
            id_name, explode("idx_columns").alias("INTEGER_IDX", "col")
        )

    def column_date_to_timestamp(self, df, column):
        date_to_udf_time_int = UserDefinedFunction(
            self.udf_milliseconds_str_to_timestamp, IntegerType()
        )
        return (
            df.withColumn(column + "_new", date_to_udf_time_int(col(column)))
            .drop(column)
            .withColumnRenamed(column + "_new", column)
        )

    @staticmethod
    def params_to_df(df, id_name):
        return df.select(
            col(id_name), explode(df["parameters"]).alias("PARAM_KEY", "PARAM_VALUE")
        )

    def generate_id_df(self, df, id_name):
        """
        generate_id_df, creates a new column <id_name>, with unique id for each row in df
        :param df: dataframe to be given id column
        :param id_name: the id name
        :return: new df with generated id
        """
        initial_id = self.start_id_map[id_name] if id_name in self.start_id_map else 0

        row_with_index = Row(*(["id"] + df.columns))
        df_columns = df.columns

        # using zipWithIndex to generate consecutive ids, rather than monotonically_increasing_ids
        # consecutive ids are desired because ids unnecessarily large will complicate future
        # appending to the same metastore (generated ids have to be bigger than the max of ids
        # already in the database
        def make_row_with_uid(columns, row, uid):
            row_dict = row.asDict()
            return row_with_index(*([uid] + [row_dict.get(c) for c in columns]))

        df_with_pk = (
            df.rdd.zipWithIndex()
            .map(lambda row_uid: make_row_with_uid(df_columns, *row_uid))
            .toDF(
                StructType(
                    [StructField("zip_id", LongType(), False)] + df.schema.fields
                )
            )
        )

        return df_with_pk.withColumn(id_name, df_with_pk.zip_id + initial_id).drop(
            "zip_id"
        )

    def extract_dbs(self, databases):
        ms_dbs_no_id = databases.select("item.*")
        ms_dbs = self.generate_id_df(ms_dbs_no_id, "DB_ID")

        # if locationUri is null, fill with empty string value
        udf_fill_location_uri = UserDefinedFunction(
            DataCatalogTransformer.udf_with_non_null_locationuri, StringType()
        )

        ms_dbs = (
            ms_dbs.select(
                "*", udf_fill_location_uri("locationUri").alias("locationUriNew")
            )
            .drop("locationUri")
            .withColumnRenamed("locationUriNew", "locationUri")
        )

        return ms_dbs

    def reformat_dbs(self, ms_dbs):
        ms_dbs = rename_columns(
            df=ms_dbs,
            rename_tuples=[("locationUri", "DB_LOCATION_URI"), ("name", "NAME")],
        )

        return ms_dbs

    def extract_tbls(self, tables, ms_dbs):
        ms_tbls_no_id = (
            tables.join(ms_dbs, tables.database == ms_dbs.NAME, "inner")
            .select(tables.database, tables.item, ms_dbs.DB_ID)
            .select("DB_ID", "database", "item.*")
        )  # database col needed for later
        ms_tbls = self.generate_id_df(ms_tbls_no_id, "TBL_ID")

        return ms_tbls

    def reformat_tbls(self, ms_tbls):
        # reformat CREATE_TIME and LAST_ACCESS_TIME
        ms_tbls = self.column_date_to_timestamp(ms_tbls, "createTime")
        ms_tbls = self.column_date_to_timestamp(ms_tbls, "lastAccessTime")

        ms_tbls = rename_columns(
            df=ms_tbls,
            rename_tuples=[
                ("database", "DB_NAME"),
                ("createTime", "CREATE_TIME"),
                ("lastAccessTime", "LAST_ACCESS_TIME"),
                ("owner", "OWNER"),
                ("retention", "RETENTION"),
                ("name", "TBL_NAME"),
                ("tableType", "TBL_TYPE"),
                ("viewExpandedText", "VIEW_EXPANDED_TEXT"),
                ("viewOriginalText", "VIEW_ORIGINAL_TEXT"),
            ],
        )

        return ms_tbls

    def get_name_for_partitions(self, ms_partitions, ms_tbls):
        tbls_for_join = ms_tbls.select("TBL_ID", "partitionKeys")

        combine_part_key_and_vals = UserDefinedFunction(
            self.udf_partition_name_from_keys_vals, StringType()
        )

        ms_partitions = (
            ms_partitions.join(
                tbls_for_join, ms_partitions.TBL_ID == tbls_for_join.TBL_ID, "inner"
            )
            .drop(tbls_for_join.TBL_ID)
            .withColumn(
                "PART_NAME",
                combine_part_key_and_vals(col("partitionKeys"), col("values")),
            )
            .drop("partitionKeys")
        )

        return ms_partitions

    def extract_partitions(self, partitions, ms_dbs, ms_tbls):
        ms_partitions = partitions.join(
            ms_dbs, partitions.database == ms_dbs.NAME, "inner"
        ).select(partitions.item, ms_dbs.DB_ID, partitions.table)

        cond = [
            ms_partitions.table == ms_tbls.TBL_NAME,
            ms_partitions.DB_ID == ms_tbls.DB_ID,
        ]

        ms_partitions = (
            ms_partitions.join(ms_tbls, cond, "inner")
            .select(ms_partitions.item, ms_tbls.TBL_ID)
            .select("TBL_ID", "item.*")
        )

        # generate PART_ID
        ms_partitions = self.generate_id_df(ms_partitions, "PART_ID")

        ms_partitions = self.get_name_for_partitions(ms_partitions, ms_tbls)

        return ms_partitions

    def reformat_partitions(self, ms_partitions):
        ms_partitions = self.column_date_to_timestamp(ms_partitions, "creationTime")
        ms_partitions = self.column_date_to_timestamp(ms_partitions, "lastAccessTime")
        ms_partitions = rename_columns(
            df=ms_partitions,
            rename_tuples=[
                ("creationTime", "CREATE_TIME"),
                ("lastAccessTime", "LAST_ACCESS_TIME"),
            ],
        )

        return ms_partitions

    def extract_sds(self, ms_tbls, ms_partitions):

        ms_tbls = ms_tbls.withColumn("ID", concat(ms_tbls.TBL_NAME, ms_tbls.DB_NAME))
        ms_partitions = ms_partitions.withColumn(
            "ID", ms_partitions.PART_ID.cast(StringType())
        )
        ms_tbls_sds = ms_tbls.select("ID", "storageDescriptor.*").withColumn(
            "type", lit("table")
        )
        ms_partitions_sds = ms_partitions.select(
            "ID", "storageDescriptor.*"
        ).withColumn("type", lit("partition"))

        ms_sds_no_id = ms_partitions_sds.union(ms_tbls_sds)

        ms_sds = self.generate_id_df(ms_sds_no_id, "SD_ID")

        ms_sds_for_join = ms_sds.select("type", "ID", "SD_ID")

        cond = [
            ms_sds_for_join.type == "partition",
            ms_sds_for_join.ID == ms_partitions.ID,
        ]
        ms_partitions = ms_partitions.join(ms_sds_for_join, cond, "inner").drop(
            "ID", "type"
        )

        cond = [ms_sds_for_join.type == "table", ms_sds_for_join.ID == ms_tbls.ID]
        ms_tbls = ms_tbls.join(ms_sds_for_join, cond, "inner").drop("ID").drop("type")

        ms_sds = ms_sds.drop("ID", "type")

        return (ms_sds, ms_tbls, ms_partitions)

    def reformat_sds(self, ms_sds):
        ms_sds = rename_columns(
            df=ms_sds,
            rename_tuples=[
                ("inputFormat", "INPUT_FORMAT"),
                ("compressed", "IS_COMPRESSED"),
                ("storedAsSubDirectories", "IS_STOREDASSUBDIRECTORIES"),
                ("location", "LOCATION"),
                ("numberOfBuckets", "NUM_BUCKETS"),
                ("outputFormat", "OUTPUT_FORMAT"),
            ],
        )

        ms_sds = self.generate_id_df(ms_sds, "CD_ID")
        ms_sds = self.generate_id_df(ms_sds, "SERDE_ID")

        return ms_sds

    def extract_from_dbs(self, hms, ms_dbs):
        ms_database_params = DataCatalogTransformer.params_to_df(ms_dbs, "DB_ID")

        hms.ms_database_params = ms_database_params
        hms.ms_dbs = ms_dbs.drop("parameters").withColumnRenamed("description", "DESC")

    def extract_from_tbls(self, hms, ms_tbls):
        ms_table_params = DataCatalogTransformer.params_to_df(ms_tbls, "TBL_ID")

        part_key_schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("type", StringType(), True),
                StructField("comment", StringType(), True),
            ]
        )

        ms_partition_keys = DataCatalogTransformer.generate_idx_for_df(
            ms_tbls, "TBL_ID", "partitionKeys", part_key_schema
        ).select("TBL_ID", "INTEGER_IDX", "col.*")

        ms_partition_keys = rename_columns(
            df=ms_partition_keys,
            rename_tuples=[
                ("name", "PKEY_NAME"),
                ("type", "PKEY_TYPE"),
                ("comment", "PKEY_COMMENT"),
            ],
        )

        hms.ms_table_params = ms_table_params
        hms.ms_partition_keys = ms_partition_keys
        hms.ms_tbls = ms_tbls.drop(
            "partitionKeys", "storageDescriptor", "parameters", "DB_NAME"
        )

    def extract_from_partitions(self, hms, ms_partitions):

        # split into table PARTITION_PARAMS
        ms_partition_params = DataCatalogTransformer.params_to_df(
            ms_partitions, "PART_ID"
        )

        # split into table PARTITION_KEY_VAL
        part_key_val_schema = StringType()

        ms_partition_key_vals = DataCatalogTransformer.generate_idx_for_df(
            ms_partitions, "PART_ID", "values", part_key_val_schema
        ).withColumnRenamed("col", "PART_KEY_VAL")

        hms.ms_partition_key_vals = ms_partition_key_vals
        hms.ms_partition_params = ms_partition_params
        hms.ms_partitions = ms_partitions.drop(
            "namespaceName", "values", "storageDescriptor", "tableName", "parameters"
        )

    def extract_from_sds(self, hms, ms_sds):
        ms_sd_params = DataCatalogTransformer.params_to_df(ms_sds, "SD_ID")

        ms_cds = ms_sds.select("CD_ID")

        ms_columns = self.extract_from_sds_columns(ms_sds)

        (ms_serdes, ms_serde_params) = self.extract_from_sds_serde_info(ms_sds)

        ms_sort_cols = self.extract_from_sds_sort_cols(ms_sds)

        hms.ms_sd_params = ms_sd_params
        hms.ms_cds = ms_cds
        hms.ms_columns = ms_columns
        hms.ms_serdes = ms_serdes
        hms.ms_serde_params = ms_serde_params
        hms.ms_sort_cols = ms_sort_cols

        self.extract_from_sds_skewed_info(hms, ms_sds)
        hms.ms_sds = ms_sds.drop(
            "parameters",
            "serdeInfo",
            "bucketColumns",
            "columns",
            "skewedInfo",
            "sortColumns",
        )

    def extract_from_sds_columns(self, ms_sds):
        COLUMN_SCHEMA = StructType(
            [
                StructField("name", StringType(), True),
                StructField("type", StringType(), True),
                StructField("comment", StringType(), True),
            ]
        )

        ms_columns = DataCatalogTransformer.generate_idx_for_df(
            ms_sds, "CD_ID", "columns", COLUMN_SCHEMA
        ).select("CD_ID", "INTEGER_IDX", "col.*")

        ms_columns = rename_columns(
            df=ms_columns,
            rename_tuples=[
                ("name", "COLUMN_NAME"),
                ("type", "TYPE_NAME"),
                ("comment", "COMMENT"),
            ],
        )

        return ms_columns

    def extract_from_sds_serde_info(self, ms_sds):
        ms_serdes = ms_sds.select("SERDE_ID", "serdeInfo.*")
        ms_serdes = rename_columns(
            df=ms_serdes,
            rename_tuples=[("name", "NAME"), ("serializationLibrary", "SLIB")],
        )

        ms_serde_params = DataCatalogTransformer.params_to_df(ms_serdes, "SERDE_ID")
        ms_serdes = ms_serdes.drop("parameters")

        return (ms_serdes, ms_serde_params)

    def extract_from_sds_skewed_info(self, hms, ms_sds):

        skewed_info = ms_sds.select("SD_ID", "skewedInfo.*")

        ms_skewed_col_names = skewed_info.select(
            "SD_ID", explode("skewedColumnNames").alias("SKEWED_COL_NAME")
        )

        # with extra field 'STRING_LIST_STR'
        skewed_col_value_loc_map = skewed_info.select(
            "SD_ID",
            explode("skewedColumnValueLocationMaps").alias(
                "STRING_LIST_STR", "LOCATION"
            ),
        )

        skewed_col_value_loc_map = self.generate_id_df(
            skewed_col_value_loc_map, "STRING_LIST_ID_KID"
        )

        udf_string_list_list = UserDefinedFunction(
            DataCatalogTransformer.udf_string_list_str_to_list,
            ArrayType(StringType(), True),
        )

        skewed_string_list_values = skewed_col_value_loc_map.select(
            col("STRING_LIST_ID_KID").alias("STRING_LIST_ID"),
            udf_string_list_list("STRING_LIST_STR").alias("STRING_LIST_LIST"),
        )

        ms_skewed_string_list_values = DataCatalogTransformer.generate_idx_for_df(
            skewed_string_list_values,
            "STRING_LIST_ID",
            "STRING_LIST_LIST",
            StringType(),
        ).withColumnRenamed("col", "STRING_LIST_VALUE")

        ms_skewed_col_value_loc_map = skewed_col_value_loc_map.drop("STRING_LIST_STR")

        ms_skewed_string_list = ms_skewed_string_list_values.select("STRING_LIST_ID")

        hms.ms_skewed_col_names = ms_skewed_col_names
        hms.ms_skewed_col_value_loc_map = ms_skewed_col_value_loc_map
        hms.ms_skewed_string_list_values = ms_skewed_string_list_values
        hms.ms_skewed_string_list = ms_skewed_string_list

    def extract_from_sds_sort_cols(self, ms_sds):

        return (
            DataCatalogTransformer.generate_idx_for_df(
                ms_sds,
                "SD_ID",
                "sortColumns",
                col_schema=StructType(
                    [
                        StructField("column", StringType(), True),
                        StructField("order", IntegerType(), True),
                    ]
                ),
            )
            .select("SD_ID", "INTEGER_IDX", "col.*")
            .withColumnRenamed("column", "COLUMN_NAME")
            .withColumnRenamed("order", "ORDER")
        )

    def get_start_id_for_id_name(self, hms):
        hms.extract_metastore()

        info_tuples = {
            ("ms_dbs", "DB_ID"),
            ("ms_tbls", "TBL_ID"),
            ("ms_sds", "SD_ID"),
            ("ms_sds", "CD_ID"),
            ("ms_sds", "SERDE_ID"),
            ("ms_partitions", "PART_ID"),
            ("ms_skewed_col_value_loc_map", "STRING_LIST_ID_KID"),
        }

        for table_name, id_name in info_tuples:
            hms_df = eval("hms." + table_name)

            if hms_df and hms_df.count() > 0:
                max_id = hms_df.select(id_name).rdd.max()[0] + 1
            else:
                max_id = 0
            self.start_id_map[id_name] = max_id

    def transform(self, hms, databases, tables, partitions):

        # for metastore tables that require unique ids, find max id (start id)
        # for rows already in each table
        self.get_start_id_for_id_name(hms)

        # establish foreign keys between dbs, tbls, partitions, sds
        ms_dbs = self.reformat_dbs(self.extract_dbs(databases))

        ms_tbls = self.reformat_tbls(self.extract_tbls(tables, ms_dbs))

        ms_partitions = self.reformat_partitions(
            self.extract_partitions(partitions, ms_dbs, ms_tbls)
        )
        (ms_sds, ms_tbls, ms_partitions) = self.extract_sds(ms_tbls, ms_partitions)
        ms_sds = self.reformat_sds(ms_sds)

        # extract child tables from above four tables and then clean up extra columns
        self.extract_from_dbs(hms, ms_dbs)
        self.extract_from_tbls(hms, ms_tbls)
        self.extract_from_sds(hms, ms_sds)
        self.extract_from_partitions(hms, ms_partitions)
