import asyncio
import unittest.mock
import websockets
from mbt.harvester import Harvester


class TestHarvester(unittest.TestCase):

    def setUp(self):
        pass

    def test_harvest(self):

        event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(event_loop)

        database_info = {
            "database_hostname": "127.0.0.1",
            "database_port": 27017,
            "database_name": "mbt",
            "collection": "coinklines"
        }

        websocket_info = {
            "url": "ws://localhost:8765"
        }

        async def hello(websocket, path):
            while True:
                await asyncio.sleep(1)

        def start_server():
            self.event_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.event_loop)
            serve = websockets.serve(hello, 'localhost', 8765)
            self.server = asyncio.get_event_loop().run_until_complete(serve)
            asyncio.get_event_loop().run_forever()

        async def start_client():
            harvester = Harvester(None, database_info, websocket_info)
            task = asyncio.create_task(harvester.harvest())
            await asyncio.sleep(2)
            print("stopping server")
            self.server.close()
            print("server stopped")
            await asyncio.sleep(2)
            self.assertEqual(task.done(), False)

        asyncio.get_event_loop().run_in_executor(None, start_server)
        asyncio.get_event_loop().run_until_complete(start_client())
        self.event_loop.stop()
        asyncio.get_event_loop().stop()

    def test_store(self):
        self.assertEqual(1, 1)
