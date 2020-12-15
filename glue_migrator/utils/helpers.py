from types import MethodType
from pyspark.sql import DataFrame
from pyspark.sql.functions import struct
from glue_migrator.utils.schema_helper import rename_columns, get_schema_type
from glue_migrator.utils.logger import Logger

logger = Logger()
logger.basicConfig()

def append(l, elem):
    """Append list with element and return the list modified"""
    if elem is not None:
        l.append(elem)
    return l


def extend(l1, l2):
    """Extend l1 with l2 and return l1 modified"""
    l1.extend(l2)
    return l1


def remove(l, elem):
    l.remove(elem)
    return l


def remove_all(l1, l2):
    return [elem for elem in l1 if elem not in l2]

def empty(df):
    return df.rdd.isEmpty()


def drop_columns(df, columns_to_drop):
    for col in columns_to_drop:
        df = df.drop(col)
    return df

def join_other_to_single_column(df, other, on, how, new_column_name):
    """
    :param df: this dataframe
    :param other: other dataframe
    :param on: the column to join on
    :param how: :param how: str, default 'inner'. One of `inner`, `outer`, `left_outer`, `right_outer`, `leftsemi`.
    :param new_column_name: the column name for all fields from the other dataframe
    :return: this dataframe, with a single new column containing all fields of the other dataframe
    :type df: DataFrame
    :type other: DataFrame
    :type new_column_name: str
    """
    other_cols = remove(other.columns, on)
    other_combined = other.select([on, struct(other_cols).alias(new_column_name)])
    return df.join(other=other_combined, on=on, how=how)

def get_options(parser, args):
    parsed, extra = parser.parse_known_args(args[1:])
    logger.info("Found arguments:", str(parsed))
    if extra:
        logger.info('Found unrecognized arguments:', extra)
    return vars(parsed)

def register_methods_to_dataframe():
        """
        Register self-defined helper methods to dataframe
        """
        DataFrame.empty = MethodType(empty, None, DataFrame)
        DataFrame.drop_columns = MethodType(drop_columns, None, DataFrame)
        DataFrame.rename_columns = MethodType(rename_columns, None, DataFrame)
        DataFrame.get_schema_type = MethodType(get_schema_type, None, DataFrame)
        DataFrame.join_other_to_single_column = MethodType(join_other_to_single_column, None, DataFrame)
