import abc
from abc import ABC
from yahoo_fin.stock_info import get_data
import logging


class PriceDataInterface(ABC):

    @abc.abstractmethod
    def authentication(self):
        """
        authenticate the connection to get price data
        :return:
        """
        pass

    @abc.abstractmethod
    def make_query(self, query: dict):
        """
        query the price data
        :param query: parameter in dict form
        :return:
        """
        pass

    @staticmethod
    def error_handling(retries=3, delay=2):
        def decorator(func):
            def wrapper(*args, **kwargs):
                attempts = 0
                while attempts < retries:
                    try:
                        return func(*args, **kwargs)
                    except Exception as error:
                        attempts += 1
                        logging.warning(f"Attempt {attempts} failed: {error}")
                        logging.info(f"Retrying in {delay} seconds..")

            return wrapper

        return decorator


class YahooFinance(PriceDataInterface):

    def authentication(self):
        """
        not required for yahoofinance
        :return:
        """
        pass

    @PriceDataInterface.error_handling(retries=2, delay=2)
    def make_query(self, query: dict):
        """
        get the stock name, the date range to extract price data
        :param query:
        :return:
        """
        result = get_data(**query)
        return result


client = YahooFinance()
query_gme = {
    "ticker": "GME",
    "start_date": "07/07/2022",
    "end_date": None,
    "index_as_date": True,
    "interval": ["1d", "1wk", "1mo"][0],

}
gme = client.make_query(query_gme)
gme.to_csv("gme_price.csv")
print(gme.tail())
