from src.tiering.policy import get_target_tier, load_tiering_rules


rules = load_tiering_rules()

test_ages = [0, 3, 6, 7, 12, 29, 30, 45]

for age in test_ages:
    tier = get_target_tier(age, rules)
    print(f"Age: {age} days -> Target tier: {tier}")
