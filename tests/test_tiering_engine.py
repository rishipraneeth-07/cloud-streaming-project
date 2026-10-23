
import unittest
from datetime import datetime, timedelta, timezone

from src.tiering.engine import calculate_age_days


class TestTieringEngine(unittest.TestCase):

    def test_recent_object_age(self):
        created_at = datetime.now(timezone.utc) - timedelta(days=2)
        age = calculate_age_days(created_at.isoformat())

        self.assertGreaterEqual(age, 2)
        self.assertLess(age, 2.01)

    def test_old_object_age(self):
        created_at = datetime.now(timezone.utc) - timedelta(days=35)
        age = calculate_age_days(created_at.isoformat())

        self.assertGreaterEqual(age, 35)
        self.assertLess(age, 35.01)

    def test_future_timestamp_raises_error(self):
        created_at = datetime.now(timezone.utc) + timedelta(days=1)

        with self.assertRaises(ValueError):
            calculate_age_days(created_at.isoformat())


if __name__ == "__main__":
    unittest.main()
