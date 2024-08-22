import os.path
import pickle
from abc import ABC
import logging
import pandas as pd
from decimal import Decimal
from definition import ROOT
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F


class PriceTransform(ABC):

    @staticmethod
    def rename_columns(price_df, params: dict) -> pd.DataFrame:
        """
        rename the column to lower cases, change column name given params
        :param price_df:
        :param params:
        :return:
        """
        pass

    @staticmethod
    def convert_timestamp_format(price_df: pd.DataFrame) -> pd.DataFrame:
        """
        convert the timestamp to datetime format
        :param price_df:
        :return:
        """
        pass

    @staticmethod
    def remove_duplicates(price_df: pd.DataFrame) -> pd.DataFrame:
        """
        remove duplicated records in price df having the same timestamp
        :param price_df:
        :return:
        """
        pass

    @staticmethod
    def convert_data_type(price_df: pd.DataFrame, params: dict) -> pd.DataFrame:
        """
        based on the given dictionary to convert the data type of the price df
        :param params:
        :param price_df:
        :return:
        """
        pass

    @staticmethod
    def fill_na(price_df: pd.DataFrame, method: str) -> pd.DataFrame:
        """
        Check the total number of na in a dataframe and fill na with given method
        :param price_df:
        :param method:
        :return:
        """
        pass

    @staticmethod
    def stage_file(price_df: pd.DataFrame, params: dict):
        """
        convert the price df to Parquet format with specified folder.
        :param params:
        :param price_df:
        :return:
        """
        pass


class YahooFinanceTransformer(PriceTransform):

    @staticmethod
    def remove_duplicates(price_df: pd.DataFrame) -> pd.DataFrame:
        price_df = price_df.reset_index()
        price_df = price_df.drop_duplicates(subset=['timestamp'])
        return price_df.set_index(['timestamp'])

    @staticmethod
    def convert_data_type(price_df: pd.DataFrame, params: dict) -> pd.DataFrame:
        price_df = price_df.astype(params)
        return price_df

    @staticmethod
    def fill_na(price_df: pd.DataFrame, method=None) -> pd.DataFrame:
        if method in ["bfill", "ffill"]:
            price_df = price_df.fillna(method=method)
        else:
            logging.info("no filling method match, would take default method ffill")
            price_df = price_df.ffill()
        return price_df

    @staticmethod
    def stage_file(price_df: pd.DataFrame,params):
        if not os.path.exists(os.path.join(ROOT,"staging")):
            os.makedirs(os.path.join(ROOT,'staging'))

        price_df.to_parquet(path=params.get('path'), engine="fastparquet")
        logging.info(f"file converted to Parquet format in staging folder")

    @staticmethod
    def convert_timestamp_format(price_df: pd.DataFrame) -> pd.DataFrame:
        price_df.index = pd.to_datetime(price_df.index)
        return price_df

    @staticmethod
    def rename_columns(price_df, params: dict) -> pd.DataFrame:
        price_df.columns = [col.lower() for col in price_df.columns]
        price_df.index.name = "timestamp"
        price_df = price_df.rename(columns=params)
        return price_df


class SparkTransformer:
    def __init__(self):
        self.spark = SparkSession.builder.appName("game_stock_tel") \
        .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:latest_version") \
        .getOrCreate()
        self.db = self.connect_to_database()

    def load_raw_file(self, path):
        df = self.spark.read.csv(path, header=True,inferSchema=True)
        return df

    @staticmethod
    def fill_na(df):
        for col in df.columns[1:]:
            windows_spec = Window.orderBy('timestamp').rowsBetween(Window.unboundedPreceding,0)
            df = df.withColumn(f"{col}_ffill",F.last(col,ignorenulls=True).over(windows_spec))
            df = df.drop(col).withColumnRenamed(f"{col}_ffill",col)
        return df

    def remove_duplicates(self, df):
        df = df.dropDuplicates(['timestamp'])
        return df

    def rename_columns(self, price_df, params: dict):
        for k,v in params.items():
            price_df = price_df.withColumnRenamed(k,v)
        return price_df

    def connect_to_database(self):
        pass

    def write_to_database(self,df):
        snowflake_options = {
            "sfURL": "your_snowflake_account_url",
            "sfUser": "your_username",
            "sfPassword": "your_password",
            "sfDatabase": "your_database",
            "sfSchema": "your_schema",
            "sfWarehouse": "your_warehouse"
        }
        df.write.parquet(os.path.join(ROOT,"staging/price.pqt"))
        df.write.format("net.snowflake.spark.snowflake") \
        .option(**snowflake_options) \
        .option("dbtable","table_name") \
        .option("temp")

        pass
# client = YahooFinanceTransformer()
# df = pd.read_csv(os.path.join(ROOT,"sample_price.csv"),index_col='Unnamed: 0')
# df['volume'].iloc[10] = None
# df['open'].iloc[12] = None
# df['high'].iloc[15:18] = None
# rename = {
#     "Open": "open",
#     "ticker": "symbol"
# }
# data_conversion = {
#     "open": float,
#     "high": float,
#     "low": float,
#     "close": float,
#     "adjclose": float,
#     "volume": float,
#     "symbol": str
# }
#
# staging_param = {"path":os.path.join(ROOT,"./staging/price.pqt")}
#
# df = client.rename_columns(df, rename)
# df = client.convert_timestamp_format(df)
# df = client.convert_data_type(df, data_conversion)
# df = client.fill_na(df, method="bfill")
# df = pd.concat([df,df.iloc[15:20]])
# df = df.sort_index()
# df = client.remove_duplicates(df)
# client.stage_file(df,staging_param)
# print(df)




"""
Below are the session running the transformation with pySpark
"""

client = SparkTransformer()
path = os.path.join(ROOT,"sample_price.csv")
rename_columns={
    "index":"timestamp",
    "Unnamed: 0":'timestamp',
    "_c0":"timestamp",
    "ticker":"symbol",
}
df = client.load_raw_file(path)
df = client.rename_columns(df, rename_columns)
df = client.fill_na(df) # passed
df = client.remove_duplicates(df)
client.write_to_database(df)
df.show()
