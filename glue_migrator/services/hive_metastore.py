

class HiveMetastore:
    """
    Class to extract data from Hive Metastore into DataFrames and write Dataframes to
    Hive Metastore. Each field represents a single Hive Metastore table.
    As a convention, the fields are prefixed by ms_ to show that it is raw Hive Metastore data
    """

    def __init__(self, connection, sql_context):
        self.connection = connection
        self.sql_context = sql_context
        self.ms_dbs = None
        self.ms_database_params = None
        self.ms_tbls = None
        self.ms_table_params = None
        self.ms_columns = None
        self.ms_bucketing_cols = None
        self.ms_sds = None
        self.ms_sd_params = None
        self.ms_serdes = None
        self.ms_serde_params = None
        self.ms_skewed_col_names = None
        self.ms_skewed_string_list = None
        self.ms_skewed_string_list_values = None
        self.ms_skewed_col_value_loc_map = None
        self.ms_sort_cols = None
        self.ms_partitions = None
        self.ms_partition_params = None
        self.ms_partition_keys = None
        self.ms_partition_key_vals = None

    def read_table(self, connection, jdbc_driver_class="com.mysql.jdbc.Driver", db_name='hive', table_name=None):
        """
        Load a JDBC table into Spark Dataframe
        """
        return self.sql_context.read.format('jdbc').options(
            url=connection['url'],
            dbtable='%s.%s' % (db_name, table_name),
            user=connection['user'],
            password=connection['password'],
            driver=jdbc_driver_class
        ).load()

    def write_table(self, connection, jdbc_driver_class, db_name='hive', table_name=None, df=None):
        """
        Write from Spark Dataframe into a JDBC table
        """
        return df.write.jdbc(
            url=connection['url'],
            table='%s.%s' % (db_name, table_name),
            mode='append',
            properties={
                'user': connection['user'],
                'password': connection['password'],
                'driver': jdbc_driver_class
            }
        )

    def extract_metastore(self):
        self.ms_dbs = self.read_table(connection=self.connection, table_name='DBS')
        self.ms_database_params = self.read_table(connection=self.connection, table_name='DATABASE_PARAMS')
        self.ms_tbls = self.read_table(connection=self.connection, table_name='TBLS')
        self.ms_table_params = self.read_table(connection=self.connection, table_name='TABLE_PARAMS')
        self.ms_columns = self.read_table(connection=self.connection, table_name='COLUMNS_V2')
        self.ms_bucketing_cols = self.read_table(connection=self.connection, table_name='BUCKETING_COLS')
        self.ms_sds = self.read_table(connection=self.connection, table_name='SDS')
        self.ms_sd_params = self.read_table(connection=self.connection, table_name='SD_PARAMS')
        self.ms_serdes = self.read_table(connection=self.connection, table_name='SERDES')
        self.ms_serde_params = self.read_table(connection=self.connection, table_name='SERDE_PARAMS')
        self.ms_skewed_col_names = self.read_table(connection=self.connection, table_name='SKEWED_COL_NAMES')
        self.ms_skewed_string_list = self.read_table(connection=self.connection, table_name='SKEWED_STRING_LIST')
        self.ms_skewed_string_list_values = self.read_table(connection=self.connection,
                                                            table_name='SKEWED_STRING_LIST_VALUES')
        self.ms_skewed_col_value_loc_map = self.read_table(connection=self.connection,
                                                           table_name='SKEWED_COL_VALUE_LOC_MAP')
        self.ms_sort_cols = self.read_table(connection=self.connection, table_name='SORT_COLS')
        self.ms_partitions = self.read_table(connection=self.connection, table_name='PARTITIONS')
        self.ms_partition_params = self.read_table(connection=self.connection, table_name='PARTITION_PARAMS')
        self.ms_partition_keys = self.read_table(connection=self.connection, table_name='PARTITION_KEYS')
        self.ms_partition_key_vals = self.read_table(connection=self.connection, table_name='PARTITION_KEY_VALS')

    def write_tables_to_metastore(self):
        """
        Write tables to Hive Metastore following the correct order
        :return:
        """
        self.ms_dbs.show()
        self.write_table(connection=self.connection, table_name='DBS', df=self.ms_dbs)
        self.write_table(connection=self.connection, table_name='DATABASE_PARAMS', df=self.ms_database_params)
        self.write_table(connection=self.connection, table_name='CDS', df=self.ms_cds)
        self.write_table(connection=self.connection, table_name='SERDES', df=self.ms_serdes)
        self.write_table(connection=self.connection, table_name='SERDE_PARAMS', df=self.ms_serde_params)
        self.write_table(connection=self.connection, table_name='COLUMNS_V2', df=self.ms_columns)
        self.write_table(connection=self.connection, table_name='SDS', df=self.ms_sds)
        self.write_table(connection=self.connection, table_name='SD_PARAMS', df=self.ms_sd_params)
        self.write_table(connection=self.connection, table_name='SKEWED_COL_NAMES', df=self.ms_skewed_col_names)
        self.write_table(connection=self.connection, table_name='SKEWED_STRING_LIST', df=self.ms_skewed_string_list)
        self.write_table(connection=self.connection, table_name='SKEWED_STRING_LIST_VALUES',
                         df=self.ms_skewed_string_list_values)
        self.write_table(connection=self.connection, table_name='SKEWED_COL_VALUE_LOC_MAP',
                         df=self.ms_skewed_col_value_loc_map)
        self.write_table(connection=self.connection, table_name='SORT_COLS',
                         df=self.ms_sort_cols)
        self.write_table(connection=self.connection, table_name='TBLS', df=self.ms_tbls)
        self.write_table(connection=self.connection, table_name='TABLE_PARAMS', df=self.ms_table_params)
        self.write_table(connection=self.connection, table_name='PARTITION_KEYS', df=self.ms_partition_keys)
        self.write_table(connection=self.connection, table_name='PARTITIONS', df=self.ms_partitions)
        self.write_table(connection=self.connection, table_name='PARTITION_PARAMS', df=self.ms_partition_params)
        self.write_table(connection=self.connection, table_name='PARTITION_KEY_VALS', df=self.ms_partition_key_vals)
