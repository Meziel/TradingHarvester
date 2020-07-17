from unittest import TestCase
from mbt.harvest_manager import HarvestManager


class TestHarvestManager(TestCase):
    def setUp(self):
        self.harvest_manager = HarvestManager()


class TestIsMissingData(TestHarvestManager):
    def test_under_one_minute(self):
        current_time = 1558828800021
        past_time = current_time - (60000 * 1.5 - 1)

        is_missing_data = HarvestManager.is_missing_data(past_time, current_time)
        self.assertEqual(is_missing_data, False)

    def test_over_one_minute(self):
        current_time = 1558828800021
        past_time = current_time - 60000 * 1.5

        is_missing_data = HarvestManager.is_missing_data(past_time, current_time)
        self.assertEqual(is_missing_data, True)

    def test_negative(self):
        current_time = 1558828800021
        past_time = current_time + (60000 * 1.5 - 1)

        is_missing_data = HarvestManager.is_missing_data(past_time, current_time)
        self.assertEqual(is_missing_data, False)
