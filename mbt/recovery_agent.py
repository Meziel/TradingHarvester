import requests
import datetime
import pymongo


class RecoveryAgent:

    def __init__(self, database_info):
        self.database_info = database_info
        self.mongo_connection = None
        self.database = None
        self.collection = None

    async def recover(self, last_close, current_close):

        self.mongo_connection = pymongo.MongoClient(host=self.database_info["database_hostname"],
                                                    port=self.database_info["database_port"])
        self.database = self.mongo_connection[self.database_info["database_name"]]
        self.collection = self.database[self.database_info["collection"]]

        url = "https://api.binance.com/api/v1/klines?symbol=BTCUSDT&interval=1m"
        request = requests.get(url=url)
        data = request.json()
        for kline in data:
            kline_close = kline[6]
            is_between = last_close < kline_close < current_close

            if is_between:
                kline = {
                    "symbol": "BTCUSDT",
                    "start_time": kline[0],
                    "close_time": kline[6],
                    "open": kline[1],
                    "close": kline[4],
                    "high": kline[2],
                    "low": kline[3],
                    "volume": kline[5]
                }
                await self.store(kline)

        self.mongo_connection.close()

    async def store(self, data):
        self.collection.insert_one(data)
        print("Recovered: " + datetime.datetime.fromtimestamp(data["close_time"] / 1000.0).strftime("%I:%M %p"))
