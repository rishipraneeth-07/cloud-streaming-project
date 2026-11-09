import unittest

from src.tiering.policy import get_target_tier, load_tiering_rules


class TestTieringPolicy(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rules = load_tiering_rules()

    def test_hot_tier_stays_hot(self):
        self.assertEqual(get_target_tier("hot-data", 0, self.rules), "hot-data")
        self.assertEqual(get_target_tier("hot-data", 6, self.rules), "hot-data")

    def test_hot_moves_to_warm_after_threshold(self):
        self.assertEqual(get_target_tier("hot-data", 7, self.rules), "warm-data")

    def test_warm_stays_warm_before_cold_threshold(self):
        self.assertEqual(get_target_tier("warm-data", 7, self.rules), "warm-data")
        self.assertEqual(get_target_tier("warm-data", 29, self.rules), "warm-data")

    def test_warm_moves_to_cold_after_threshold(self):
        self.assertEqual(get_target_tier("warm-data", 30, self.rules), "cold-data")

    def test_cold_never_moves_to_hotter_tier(self):
        self.assertEqual(get_target_tier("cold-data", 100, self.rules), "cold-data")

    def test_negative_age_raises_error(self):
        with self.assertRaises(ValueError):
            get_target_tier("hot-data", -1, self.rules)

    def test_unknown_bucket_raises_error(self):
        with self.assertRaises(ValueError):
            get_target_tier("unknown", 10, self.rules)


if __name__ == "__main__":
    unittest.main()
