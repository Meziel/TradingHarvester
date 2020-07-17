import pymongo
import datetime
import websockets
import json


class Harvester:

    def __init__(self, harvest_manager, database_info, websocket_info):
        self.harvest_manager = harvest_manager
        self.database_info = database_info
        self.websocket_info = websocket_info
        self.mongo_connection = None
        self.database = None
        self.collection = None

    async def harvest(self):
        print("Connecting")

        while True:
            async with websockets.connect(self.websocket_info["url"]) as websocket:
                print("Connected")

                self.mongo_connection = pymongo.MongoClient(host=self.database_info["database_hostname"],
                                                            port=self.database_info["database_port"])
                self.database = self.mongo_connection[self.database_info["database_name"]]
                self.collection = self.database[self.database_info["collection"]]

                # get klines from Binance
                async for message in websocket:
                    message_kline = json.loads(message)["data"]["k"]

                    is_kline_closed = message_kline["x"]
                    if is_kline_closed:

                        # create data to store
                        kline = {
                            "symbol": message_kline["s"],
                            "start_time": message_kline["t"],
                            "close_time": message_kline["T"],
                            "open": message_kline["o"],
                            "close": message_kline["c"],
                            "high": message_kline["h"],
                            "low": message_kline["l"],
                            "volume": message_kline["v"]
                        }

                        # store message
                        await self.store(kline)
                #websockets.exceptions.ConnectionClosed

                self.mongo_connection.close()

    async def store(self, data):
        self.collection.insert_one(data)
        if self.harvest_manager is not None:
            await self.harvest_manager.queue.put(data)
        print("Update: " + datetime.datetime.now().strftime("%I:%M %p"))
