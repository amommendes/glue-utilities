
from pyspark.sql.types import StringType, StructField, StructType, LongType, ArrayType, MapType, IntegerType
from pyspark.sql.functions import UserDefinedFunction, struct, lit
from glue_migrator.utils.helper_functions import remove
from glue_migrator.utils.schema_helper import rename_columns

class HiveMetastoreTransformer:

    def __init__(self, sc, sql_context, db_prefix, table_prefix):
        self.sc = sc
        self.sql_context = sql_context
        self.db_prefix = db_prefix
        self.table_prefix = table_prefix

    def transform_params(self, params_df, id_col, key='PARAM_KEY', value='PARAM_VALUE'):
        """
        Transform a PARAMS table dataframe to dataframe of 2 columns: (id, Map<key, value>)
        :param params_df: dataframe of PARAMS table
        :param id_col: column name for id field
        :param key: column name for key
        :param value: column name for value
        :return: dataframe of params in map
        """
        return self.kv_pair_to_map(params_df, id_col, key, value, 'parameters')

    def kv_pair_to_map(self, df, id_col, key, value, map_col_name):
        def merge_dict(dict1, dict2):
            dict1.update(dict2)
            return dict1

        def remove_none_key(dictionary):
            if None in dictionary:
                del dictionary[None]
            return dictionary

        id_type = df.get_schema_type(id_col)
        map_type = MapType(keyType=df.get_schema_type(key), valueType=df.get_schema_type(value))
        output_schema = StructType([StructField(name=id_col, dataType=id_type, nullable=False),
                                    StructField(name=map_col_name, dataType=map_type)])

        return self.sql_context.createDataFrame(
            df.rdd.map(lambda row: (row[id_col], {row[key]: row[value]})).reduceByKey(merge_dict).map(
                lambda (id_name, dictionary): (id_name, remove_none_key(dictionary))), output_schema)

    def join_with_params(self, df, df_params, id_col):
        df_params_map = self.transform_params(params_df=df_params, id_col=id_col)
        df_with_params = df.join(other=df_params_map, on=id_col, how='left_outer')
        return df_with_params

    def transform_df_with_idx(self, df, id_col, idx, payloads_column_name, payload_type, payload_func):
        """
        Aggregate dataframe by ID, create a single PAYLOAD column where each row is a list of data sorted by IDX, and
        each element is a payload created by payload_func. Example:

        Input:
        df =
        +---+---+----+----+
        | ID|IDX|COL1|COL2|
        +---+---+----+----+
        |  1|  2|   1|   1|
        |  1|  1|   2|   2|
        |  2|  1|   3|   3|
        +---+---+----+----+
        id = 'ID'
        idx = 'IDX'
        payload_list_name = 'PAYLOADS'
        payload_func = row.COL1 + row.COL2

        Output:
        +------+--------+
        |    ID|PAYLOADS|
        +------+--------+
        |     1| [4, 2] |
        |     2|    [6] |
        +------+--------+

        The method assumes (ID, IDX) is input table primary key. ID and IDX values cannot be None

        :param df: dataframe with id and idx columns
        :param id_col: name of column for id
        :param idx: name of column for sort index
        :param payloads_column_name: the column name for payloads column in the output dataframe
        :param payload_func: the function to transform an input row to a payload object
        :param payload_type: the schema type for a single payload object
        :return: output dataframe with data grouped by id and sorted by idx
        """
        rdd_result = df.rdd.map(lambda row: (row[id_col], (row[idx], payload_func(row)))) \
            .aggregateByKey([], append, extend) \
            .map(lambda (id_column, list_with_idx): (id_column, sorted(list_with_idx, key=lambda t: t[0]))) \
            .map(lambda (id_column, list_with_idx): (id_column, [payload for index, payload in list_with_idx]))

        schema = StructType([StructField(name=id_col, dataType=LongType(), nullable=False),
                             StructField(name=payloads_column_name, dataType=ArrayType(elementType=payload_type))])
        return self.sql_context.createDataFrame(rdd_result, schema)

    def transform_ms_partition_keys(self, ms_partition_keys):
        return self.transform_df_with_idx(df=ms_partition_keys,
                                          id_col='TBL_ID',
                                          idx='INTEGER_IDX',
                                          payloads_column_name='partitionKeys',
                                          payload_type=StructType([
                                              StructField(name='name', dataType=StringType()),
                                              StructField(name='type', dataType=StringType()),
                                              StructField(name='comment', dataType=StringType())]),
                                          payload_func=lambda row: (
                                              row['PKEY_NAME'], row['PKEY_TYPE'], row['PKEY_COMMENT']))

    def transform_ms_partition_key_vals(self, ms_partition_key_vals):
        return self.transform_df_with_idx(df=ms_partition_key_vals,
                                          id_col='PART_ID',
                                          idx='INTEGER_IDX',
                                          payloads_column_name='values',
                                          payload_type=StringType(),
                                          payload_func=lambda row: row['PART_KEY_VAL'])

    def transform_ms_bucketing_cols(self, ms_bucketing_cols):
        return self.transform_df_with_idx(df=ms_bucketing_cols,
                                          id_col='SD_ID',
                                          idx='INTEGER_IDX',
                                          payloads_column_name='bucketColumns',
                                          payload_type=StringType(),
                                          payload_func=lambda row: row['BUCKET_COL_NAME'])

    def transform_ms_columns(self, ms_columns):
        return self.transform_df_with_idx(df=ms_columns,
                                          id_col='CD_ID',
                                          idx='INTEGER_IDX',
                                          payloads_column_name='columns',
                                          payload_type=StructType([
                                              StructField(name='name', dataType=StringType()),
                                              StructField(name='type', dataType=StringType()),
                                              StructField(name='comment', dataType=StringType())]),
                                          payload_func=lambda row: (
                                              row['COLUMN_NAME'], row['TYPE_NAME'], row['COMMENT']))

    def transform_ms_skewed_col_names(self, ms_skewed_col_names):
        return self.transform_df_with_idx(df=ms_skewed_col_names,
                                          id_col='SD_ID',
                                          idx='INTEGER_IDX',
                                          payloads_column_name='skewedColumnNames',
                                          payload_type=StringType(),
                                          payload_func=lambda row: row['SKEWED_COL_NAME'])

    def transform_ms_skewed_string_list_values(self, ms_skewed_string_list_values):
        return self.transform_df_with_idx(df=ms_skewed_string_list_values,
                                          id_col='STRING_LIST_ID',
                                          idx='INTEGER_IDX',
                                          payloads_column_name='skewedColumnValuesList',
                                          payload_type=StringType(),
                                          payload_func=lambda row: row['STRING_LIST_VALUE'])

    def transform_ms_sort_cols(self, sort_cols):
        return self.transform_df_with_idx(df=sort_cols,
                                          id_col='SD_ID',
                                          idx='INTEGER_IDX',
                                          payloads_column_name='sortColumns',
                                          payload_type=StructType([
                                              StructField(name='column', dataType=StringType()),
                                              StructField(name='order', dataType=IntegerType())]),
                                          payload_func=lambda row: (row['COLUMN_NAME'], row['ORDER']))

    @staticmethod
    def udf_escape_chars(param_value):
        ret_param_value = param_value.replace('\\', '\\\\')\
            .replace('|', '\\|')\
            .replace('"', '\\"')\
            .replace('{', '\\{')\
            .replace(':', '\\:')\
            .replace('}', '\\}')

        return ret_param_value

    @staticmethod
    def udf_skewed_values_to_str():
        return UserDefinedFunction(lambda values: ''.join(
            map(lambda v: '' if v is None else '%d%%%s' % (len(v), v), values)
        ), StringType())

    @staticmethod
    def modify_column_by_udf(df, udf, column_to_modify, new_column_name=None):
        """
        transform a column of the dataframe with the user-defined function, keeping all other columns unchanged.
        :param new_column_name: new column name. If None, old column name will be used
        :param df: dataframe
        :param udf: user-defined function
        :param column_to_modify: the name of the column to modify.
        :type column_to_modify: str
        :return: the dataframe with single column modified
        """
        if new_column_name is None:
            new_column_name = column_to_modify
        return df.select(
            *[udf(column).alias(new_column_name) if column == column_to_modify else column for column in df.columns])

    @staticmethod
    def s3a_or_s3n_to_s3_in_location(df, location_col_name):
        """
        For a dataframe with a column containing location strings, for any location "s3a://..." or "s3n://...", replace
        them with "s3://...".
        :param df: dataframe
        :param location_col_name: the name of the column containing location, must be string type
        :return: dataframe with location columns where all "s3a" or "s3n" protocols are replaced by "s3"
        """
        udf = UserDefinedFunction(
            lambda location: None if location is None else re.sub(r'^s3[a|n]:\/\/', 's3://', location),
            StringType())
        return HiveMetastoreTransformer.modify_column_by_udf(df=df, udf=udf, column_to_modify=location_col_name)

    @staticmethod
    def add_prefix_to_column(df, column_to_modify, prefix):
        if prefix is None or prefix == '':
            return df
        udf = UserDefinedFunction(lambda col: prefix + col, StringType())
        return HiveMetastoreTransformer.modify_column_by_udf(df=df, udf=udf, column_to_modify=column_to_modify)

    @staticmethod
    def utc_timestamp_to_iso8601_time(df, date_col_name, new_date_col_name):
        """
        Tape DataCatalog writer uses Gson to parse Date column. According to Gson deserializer, (https://goo.gl/mQdXuK)
        it uses either java DateFormat or ISO-8601 format. I convert Date to be compatible with java DateFormat
        :param df: dataframe with a column of unix timestamp in seconds of number type
        :param date_col_name: timestamp column
        :param new_date_col_name: new column with converted timestamp, if None, old column name is used
        :type df: DataFrame
        :type date_col_name: str
        :type new_date_col_name: str
        :return: dataframe with timestamp column converted to string representation of time
        """

        def convert_time(timestamp):
            if timestamp is None:
                return None
            return datetime.fromtimestamp(timestamp=float(timestamp), tz=UTC()).strftime("%b %d, %Y %I:%M:%S %p")

        udf_time_int_to_date = UserDefinedFunction(convert_time, StringType())
        return HiveMetastoreTransformer.modify_column_by_udf(df, udf_time_int_to_date, date_col_name, new_date_col_name)

    @staticmethod
    def transform_timestamp_cols(df, date_cols_map):
        """
        Call timestamp_int_to_iso8601_time in batch, rename all time columns in date_cols_map keys.
        :param df: dataframe with columns of unix timestamp
        :param date_cols_map: map from old column name to new column name
        :type date_cols_map: dict
        :return: dataframe
        """
        for k, v in date_cols_map.iteritems():
            df = HiveMetastoreTransformer.utc_timestamp_to_iso8601_time(df, k, v)
        return df

    @staticmethod
    def fill_none_with_empty_list(df, column):
        """
        Given a column of array type, fill each None value with empty list.
        This is not doable by df.na.fill(), Spark will throw Unsupported value type java.util.ArrayList ([]).
        :param df: dataframe with array type
        :param column: column name string, the column must be array type
        :return: dataframe that fills None with empty list for the given column
        """
        return HiveMetastoreTransformer.modify_column_by_udf(
            df=df,
            udf=UserDefinedFunction(
                lambda lst: [] if lst is None else lst,
                get_schema_type(df, column)
            ),
            column_to_modify=column,
            new_column_name=column
        )

    @staticmethod
    def join_dbs_tbls(ms_dbs, ms_tbls):
        return ms_dbs.select('DB_ID', 'NAME').join(other=ms_tbls, on='DB_ID', how='inner')

    def transform_skewed_values_and_loc_map(self, ms_skewed_string_list_values, ms_skewed_col_value_loc_map):
        # columns: (STRING_LIST_ID:BigInt, skewedColumnValuesList:List[String])
        skewed_values_list = self.transform_ms_skewed_string_list_values(ms_skewed_string_list_values)

        # columns: (STRING_LIST_ID:BigInt, skewedColumnValuesStr:String)
        skewed_value_str = self.modify_column_by_udf(df=skewed_values_list,
                                                     udf=HiveMetastoreTransformer.udf_skewed_values_to_str(),
                                                     column_to_modify='skewedColumnValuesList',
                                                     new_column_name='skewedColumnValuesStr')

        # columns: (SD_ID: BigInt, STRING_LIST_ID_KID: BigInt, STRING_LIST_ID: BigInt,
        # LOCATION: String, skewedColumnValuesStr: String)
        skewed_value_str_with_loc = ms_skewed_col_value_loc_map \
            .join(other=skewed_value_str,
                  on=[ms_skewed_col_value_loc_map['STRING_LIST_ID_KID'] == skewed_value_str['STRING_LIST_ID']],
                  how='inner')

        # columns: (SD_ID: BigInt, skewedColumnValueLocationMaps: Map[String, String])
        skewed_column_value_location_maps = self.kv_pair_to_map(df=skewed_value_str_with_loc,
                                                                id_col='SD_ID',
                                                                key='skewedColumnValuesStr',
                                                                value='LOCATION',
                                                                map_col_name='skewedColumnValueLocationMaps')

        # columns: (SD_ID: BigInt, skewedColumnValues: List[String])
        skewed_column_values = self.sql_context.createDataFrame(
            data=skewed_value_str_with_loc.rdd.map(
                lambda row: (row['SD_ID'], row['skewedColumnValues'])
            ).aggregateByKey([], append, extend),
            schema=StructType([
                StructField(name='SD_ID', dataType=LongType()),
                StructField(name='skewedColumnValues', dataType=ArrayType(elementType=StringType()))
            ]))

        return skewed_column_values, skewed_column_value_location_maps

    def transform_skewed_info(self, ms_skewed_col_names, ms_skewed_string_list_values, ms_skewed_col_value_loc_map):
        (skewed_column_values, skewed_column_value_location_maps) = self.transform_skewed_values_and_loc_map(
            ms_skewed_string_list_values, ms_skewed_col_value_loc_map)

        # columns: (SD_ID: BigInt, skewedColumnNames: List[String])
        skewed_column_names = self.transform_ms_skewed_col_names(ms_skewed_col_names)

        # columns: (SD_ID: BigInt, skewedColumnNames: List[String], skewedColumnValues: List[String],
        # skewedColumnValueLocationMaps: Map[String, String])
        skewed_info = skewed_column_names \
            .join(other=skewed_column_value_location_maps, on='SD_ID', how='outer') \
            .join(other=skewed_column_values, on='SD_ID', how='outer')
        return skewed_info

    # TODO: remove when escape special characters fix in DatacatalogWriter is pushed to production.
    def transform_param_value(self, df):
        udf_escape_chars = UserDefinedFunction(HiveMetastoreTransformer.udf_escape_chars, StringType())

        return df.select('*', udf_escape_chars('PARAM_VALUE').alias('PARAM_VALUE_ESCAPED'))\
            .drop('PARAM_VALUE')\
            .withColumnRenamed('PARAM_VALUE_ESCAPED', 'PARAM_VALUE')

    def transform_ms_serde_info(self, ms_serdes, ms_serde_params):
        escaped_serde_params = self.transform_param_value(ms_serde_params)

        serde_with_params = self.join_with_params(df=ms_serdes, df_params=escaped_serde_params, id_col='SERDE_ID')
        serde_info = serde_with_params.rename_columns(rename_tuples=[
            ('NAME', 'name'),
            ('SLIB', 'serializationLibrary')
        ])
        return serde_info

    def transform_storage_descriptors(self, ms_sds, ms_sd_params, ms_columns, ms_bucketing_cols, ms_serdes,
                                      ms_serde_params, ms_skewed_col_names, ms_skewed_string_list_values,
                                      ms_skewed_col_value_loc_map, ms_sort_cols):
        bucket_columns = self.transform_ms_bucketing_cols(ms_bucketing_cols)
        columns = self.transform_ms_columns(ms_columns)
        parameters = self.transform_params(params_df=ms_sd_params, id_col='SD_ID')
        serde_info = self.transform_ms_serde_info(ms_serdes=ms_serdes, ms_serde_params=ms_serde_params)
        skewed_info = self.transform_skewed_info(ms_skewed_col_names=ms_skewed_col_names,
                                                 ms_skewed_string_list_values=ms_skewed_string_list_values,
                                                 ms_skewed_col_value_loc_map=ms_skewed_col_value_loc_map)
        sort_columns = self.transform_ms_sort_cols(ms_sort_cols)

        storage_descriptors_joined = ms_sds \
            .join(other=bucket_columns, on='SD_ID', how='left_outer') \
            .join(other=columns, on='CD_ID', how='left_outer') \
            .join(other=parameters, on='SD_ID', how='left_outer') \
            .join_other_to_single_column(other=serde_info, on='SERDE_ID', how='left_outer',
                                         new_column_name='serdeInfo') \
            .join_other_to_single_column(other=skewed_info, on='SD_ID', how='left_outer',
                                         new_column_name='skewedInfo') \
            .join(other=sort_columns, on='SD_ID', how='left_outer')

        storage_descriptors_s3_location_fixed = \
            HiveMetastoreTransformer.s3a_or_s3n_to_s3_in_location(storage_descriptors_joined, 'LOCATION')
        storage_descriptors_renamed = storage_descriptors_s3_location_fixed.rename_columns(rename_tuples=[
            ('INPUT_FORMAT', 'inputFormat'),
            ('OUTPUT_FORMAT', 'outputFormat'),
            ('LOCATION', 'location'),
            ('NUM_BUCKETS', 'numberOfBuckets'),
            ('IS_COMPRESSED', 'compressed'),
            ('IS_STOREDASSUBDIRECTORIES', 'storedAsSubDirectories')
        ])

        storage_descriptors_with_empty_sorted_cols = HiveMetastoreTransformer.fill_none_with_empty_list(
            storage_descriptors_renamed, 'sortColumns')
        storage_descriptors_final = storage_descriptors_with_empty_sorted_cols.drop_columns(['SERDE_ID', 'CD_ID'])
        return storage_descriptors_final

    def transform_tables(self, db_tbl_joined, ms_table_params, storage_descriptors, ms_partition_keys):
        tbls_date_transformed = self.transform_timestamp_cols(db_tbl_joined, date_cols_map={
            'CREATE_TIME': 'createTime',
            'LAST_ACCESS_TIME': 'lastAccessTime'
        })
        tbls_with_params = self.join_with_params(df=tbls_date_transformed, df_params=self.transform_param_value(ms_table_params), id_col='TBL_ID')
        partition_keys = self.transform_ms_partition_keys(ms_partition_keys)

        tbls_joined = tbls_with_params\
            .join(other=partition_keys, on='TBL_ID', how='left_outer')\
            .join_other_to_single_column(other=storage_descriptors, on='SD_ID', how='left_outer',
                                         new_column_name='storageDescriptor')

        tbls_renamed = rename_columns(df=tbls_joined, rename_tuples=[
            ('NAME', 'database'),
            ('TBL_NAME', 'name'),
            ('TBL_TYPE', 'tableType'),
            ('CREATE_TIME', 'createTime'),
            ('LAST_ACCESS_TIME', 'lastAccessTime'),
            ('OWNER', 'owner'),
            ('RETENTION', 'retention'),
            ('VIEW_EXPANDED_TEXT', 'viewExpandedText'),
            ('VIEW_ORIGINAL_TEXT', 'viewOriginalText'),
        ])

        tbls_dropped_cols = tbls_renamed.drop_columns(['DB_ID', 'TBL_ID', 'SD_ID', 'LINK_TARGET_ID'])
        tbls_drop_invalid = tbls_dropped_cols.na.drop(how='any', subset=['name', 'database'])
        tbls_with_empty_part_cols = HiveMetastoreTransformer.fill_none_with_empty_list(
            tbls_drop_invalid, 'partitionKeys')
        tbls_final = tbls_with_empty_part_cols.select(
            'database', struct(remove(tbls_dropped_cols.columns, 'database')).alias('item')
        ).withColumn('type', lit('table'))
        return tbls_final

    def transform_partitions(self, db_tbl_joined, ms_partitions, storage_descriptors, ms_partition_params,
                             ms_partition_key_vals):
        parts_date_transformed = self.transform_timestamp_cols(df=ms_partitions, date_cols_map={
            'CREATE_TIME': 'creationTime',
            'LAST_ACCESS_TIME': 'lastAccessTime'
        })
        db_tbl_names = db_tbl_joined.select(db_tbl_joined['NAME'].alias('namespaceName'),
                                            db_tbl_joined['TBL_NAME'].alias('tableName'), 'DB_ID', 'TBL_ID')
        parts_with_db_tbl = parts_date_transformed.join(other=db_tbl_names, on='TBL_ID', how='inner')
        parts_with_params = self.join_with_params(df=parts_with_db_tbl, df_params=self.transform_param_value(ms_partition_params), id_col='PART_ID')
        parts_with_sd = parts_with_params.join_other_to_single_column(
            other=storage_descriptors, on='SD_ID', how='left_outer', new_column_name='storageDescriptor')
        part_values = self.transform_ms_partition_key_vals(ms_partition_key_vals)
        parts_with_values = parts_with_sd.join(other=part_values, on='PART_ID', how='left_outer')
        parts_renamed = rename_columns(df=parts_with_values, rename_tuples=[
            ('CREATE_TIME', 'createTime'),
            ('LAST_ACCESS_TIME', 'lastAccessTime')
        ])
        parts_dropped_cols = parts_renamed.drop_columns([
            'DB_ID', 'TBL_ID', 'PART_ID', 'SD_ID', 'PART_NAME', 'LINK_TARGET_ID'
        ])
        parts_drop_invalid = parts_dropped_cols.na.drop(how='any', subset=['values', 'namespaceName', 'tableName'])
        parts_final = parts_drop_invalid.select(
            parts_drop_invalid['namespaceName'].alias('database'),
            parts_drop_invalid['tableName'].alias('table'),
            struct(parts_drop_invalid.columns).alias('item')
        ).withColumn('type', lit('partition'))
        return parts_final

    def transform_databases(self, ms_dbs, ms_database_params):
        dbs_with_params = self.join_with_params(df=ms_dbs, df_params=ms_database_params, id_col='DB_ID')
        dbs_renamed = rename_columns(df=dbs_with_params, rename_tuples=[
            ('NAME', 'name'),
            ('DESC', 'description'),
            ('DB_LOCATION_URI', 'locationUri')
        ])
        dbs_dropped_cols = dbs_renamed.drop_columns(['DB_ID', 'OWNER_NAME', 'OWNER_TYPE'])
        dbs_drop_invalid = dbs_dropped_cols.na.drop(how='any', subset=['name'])
        dbs_final = dbs_drop_invalid.select(struct(dbs_dropped_cols.columns).alias('item')) \
            .withColumn('type', lit('database'))
        return dbs_final

    def transform(self, hive_metastore):
        dbs_prefixed = HiveMetastoreTransformer.add_prefix_to_column(hive_metastore.ms_dbs, 'NAME', self.db_prefix)
        tbls_prefixed = HiveMetastoreTransformer.add_prefix_to_column(
            hive_metastore.ms_tbls, 'TBL_NAME', self.table_prefix)

        databases = self.transform_databases(
            ms_dbs=dbs_prefixed,
            ms_database_params=hive_metastore.ms_database_params)

        db_tbl_joined = HiveMetastoreTransformer.join_dbs_tbls(ms_dbs=dbs_prefixed, ms_tbls=tbls_prefixed)

        storage_descriptors = self.transform_storage_descriptors(
            ms_sds=hive_metastore.ms_sds,
            ms_sd_params=hive_metastore.ms_sd_params,
            ms_columns=hive_metastore.ms_columns,
            ms_bucketing_cols=hive_metastore.ms_bucketing_cols,
            ms_serdes=hive_metastore.ms_serdes,
            ms_serde_params=hive_metastore.ms_serde_params,
            ms_skewed_col_names=hive_metastore.ms_skewed_col_names,
            ms_skewed_string_list_values=hive_metastore.ms_skewed_string_list_values,
            ms_skewed_col_value_loc_map=hive_metastore.ms_skewed_col_value_loc_map,
            ms_sort_cols=hive_metastore.ms_sort_cols)

        tables = self.transform_tables(
            db_tbl_joined=db_tbl_joined,
            ms_table_params=hive_metastore.ms_table_params,
            storage_descriptors=storage_descriptors,
            ms_partition_keys=hive_metastore.ms_partition_keys)

        partitions = self.transform_partitions(
            db_tbl_joined=db_tbl_joined,
            ms_partitions=hive_metastore.ms_partitions,
            storage_descriptors=storage_descriptors,
            ms_partition_params=hive_metastore.ms_partition_params,
            ms_partition_key_vals=hive_metastore.ms_partition_key_vals)

        return databases, tables, partitions

