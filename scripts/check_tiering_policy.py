from src.tiering.policy import get_target_tier, load_tiering_rules

rules = load_tiering_rules()

for source_bucket in ("hot-data", "warm-data", "cold-data"):
    for age_days in (0, 3, 6, 7, 12, 29, 30, 45):
        target = get_target_tier(source_bucket, age_days, rules)
        print(
            f"Source: {source_bucket}, age: {age_days} days "
            f"-> target: {target}"
        )
