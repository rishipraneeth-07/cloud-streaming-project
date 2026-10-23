
import json
from pathlib import Path


CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "tiering_rules.json"


def load_tiering_rules():
    with open(CONFIG_PATH, "r", encoding="utf-8") as file:
        return json.load(file)


def get_target_tier(age_days, rules):
    if age_days < 0:
        raise ValueError("Dataset age cannot be negative")

    hot_max_age = rules["hot"]["max_age_days"]
    warm_max_age = rules["warm"]["max_age_days"]

    if age_days < hot_max_age:
        return "hot"

    if age_days < warm_max_age:
        return "warm"

    return "cold"

