from mbt.harvester import Harvester
from mbt.recovery_agent import RecoveryAgent
import asyncio
import pymongo
import datetime
import json


class HarvestManager:

    def __init__(self):
        self.harvester = None
        self.mongo_connection = None
        self.database_info = None
        self.websocket_info = None
        self.database = None
        self.collection = None
        self.queue = None

    @staticmethod
    def is_missing_data(last_time_ms, current_time_ms):
        last_time_m = last_time_ms / 1000.0 / 60.0
        current_time_m = current_time_ms / 1000.0 / 60.0

        return int(round(current_time_m - last_time_m)) > 1

    def loadConfigurationFile(self):
        with open("config.json", "r") as configurationFile:
            config = json.loads(configurationFile.read())
            self.database_info = config["database_info"]
            self.websocket_info = config["websocket_info"]

    async def run(self):

        self.loadConfigurationFile()

        self.mongo_connection = pymongo.MongoClient(host=self.database_info["database_hostname"],
                                                    port=self.database_info["database_port"])
        self.database = self.mongo_connection[self.database_info["database_name"]]
        self.collection = self.database[self.database_info["collection"]]

        self.queue = asyncio.Queue()

        # get last kline close time
        last_klines = list(self.collection.find({}, {"close_time": 1, "_id": 0}).sort("close_time", -1).limit(1))
        last_kline = None
        if len(last_klines) is 1:
            last_kline = last_klines[0]
            last_kline_time = last_kline["close_time"]
            print("Last update from db: " + datetime.datetime.fromtimestamp(last_kline_time / 1000.0).strftime("%I:%M %p"))
        else:
            last_kline_time = 0
            print("Last update from db: Never")

        # create harvester
        harvester = Harvester(self, self.database_info, self.websocket_info)
        asyncio.create_task(harvester.harvest())
        while True:
            # wait until harvester gets kline
            kline = await self.queue.get()
            # check for gaps in time
            if last_kline is None or self.is_missing_data(last_kline_time, kline["close_time"]):
                # missing data. create recovery agent
                print("Detected missing klines. Filling gaps.")
                await RecoveryAgent(self.database_info).recover(last_kline_time, kline["close_time"])

            last_kline_time = kline["close_time"]


def main():
    asyncio.run(HarvestManager().run())


if __name__ == "__main__":
    main()

