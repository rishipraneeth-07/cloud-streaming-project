
import unittest

from src.tiering.policy import get_target_tier, load_tiering_rules


class TestTieringPolicy(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.rules = load_tiering_rules()["tiers"]

    def test_hot_tier(self):
        self.assertEqual(get_target_tier(0, self.rules), "hot")
        self.assertEqual(get_target_tier(6, self.rules), "hot")

    def test_warm_tier(self):
        self.assertEqual(get_target_tier(7, self.rules), "warm")
        self.assertEqual(get_target_tier(29, self.rules), "warm")

    def test_cold_tier(self):
        self.assertEqual(get_target_tier(30, self.rules), "cold")
        self.assertEqual(get_target_tier(100, self.rules), "cold")

    def test_negative_age_raises_error(self):
        with self.assertRaises(ValueError):
            get_target_tier(-1, self.rules)


if __name__ == "__main__":
    unittest.main()

