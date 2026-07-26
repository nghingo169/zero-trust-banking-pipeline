import unittest
from src.replay_validation.customer import REPLAY_DATES, validate_staged_prefix

class CustomerDevelopmentReplayTests(unittest.TestCase):
    def test_customer_six_day_prefix(self):
        self.assertEqual(len(REPLAY_DATES), 6)
        validate_staged_prefix("2026-07-10", REPLAY_DATES[:5])
