
import json
from pathlib import Path


CONFIG_PATH = (
    Path(__file__).resolve().parents[2]
    / "config"
    / "tiering_rules.json"
)


def load_tiering_rules():
    with open(CONFIG_PATH, "r", encoding="utf-8") as file:
        rules = json.load(file)

    if "tiers" not in rules:
        raise ValueError("Missing 'tiers' in tiering rules")

    return rules


def get_target_tier(source_bucket, age_days, rules):
    if age_days < 0:
        raise ValueError("Dataset age cannot be negative")

    tiers = rules["tiers"]

    required_tiers = ("hot", "warm", "cold")

    for tier in required_tiers:
        if tier not in tiers or "bucket" not in tiers[tier]:
            raise ValueError(f"Invalid tier configuration: {tier}")

    bucket_to_tier = {
        tiers[tier]["bucket"]: tier
        for tier in required_tiers
    }

    if source_bucket not in bucket_to_tier:
        raise ValueError(f"Unknown source bucket: {source_bucket}")

    hot_max_age = tiers["hot"]["max_age_days"]
    warm_max_age = tiers["warm"]["max_age_days"]

    if age_days < hot_max_age:
        target_tier = "hot"
    elif age_days < warm_max_age:
        target_tier = "warm"
    else:
        target_tier = "cold"

    tier_order = {
        "hot": 0,
        "warm": 1,
        "cold": 2,
    }

    source_tier = bucket_to_tier[source_bucket]

    # Never move an object back towards a hotter tier.
    if tier_order[target_tier] <= tier_order[source_tier]:
        return source_bucket

    return tiers[target_tier]["bucket"]

