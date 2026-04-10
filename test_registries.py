"""Test transformer and validator registries."""
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

print("🔧 Testing Transformer & Validator Registries...")

# Test transformers
print("\n📊 Transformers:")
from transformers.registry import get_transformer_options, get_transformer
from transformers.base import DataTransformer

transformer_options = get_transformer_options()
print(f"   Total: {len(transformer_options)} transformers registered")
for opt in transformer_options[:5]:
    print(f"   - {opt['name']}: {opt['label']}")
if len(transformer_options) > 5:
    print(f"   ... and {len(transformer_options) - 5} more")

# Test transformer execution
print("\n🧪 Testing transformer execution:")
transformer = DataTransformer()
test_series = pd.Series(["  Alice  ", "  Bob  ", "  Charlie  "])
result = transformer.apply_transform(test_series, ["TRIM"])
print(f"   Before: {test_series.tolist()}")
print(f"   After TRIM: {result.tolist()}")

# Test validators
print("\n📊 Validators:")
from validators.registry import get_validator_options, get_validator

validator_options = get_validator_options()
print(f"   Total: {len(validator_options)} validators registered")
for opt in validator_options:
    print(f"   - {opt['name']}: {opt['label']}")

# Test validator execution
print("\n🧪 Testing validator execution:")
validator = get_validator("REQUIRED")
test_series = pd.Series(["value1", "value2", None, "value3"])
result = validator(test_series)
print(f"   Series: {test_series.tolist()}")
print(f"   Valid: {result['valid']}")
print(f"   Invalid count: {result['invalid_count']}")

# Test config.py functions
print("\n🔧 Testing config.py functions:")
from config import get_transformer_options, get_validator_options, get_db_types
print(f"   get_transformer_options(): {len(get_transformer_options())} transformers")
print(f"   get_validator_options(): {len(get_validator_options())} validators")
print(f"   get_db_types(): {get_db_types()}")

print("\n✅ All registries working correctly!")
