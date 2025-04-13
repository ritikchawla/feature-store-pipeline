from datetime import timedelta
from feast import Entity, Feature, FeatureView, FileSource, ValueType
from feast.types import Float32, Int64

# Define the customer entity
customer = Entity(
    name="customer",
    value_type=ValueType.INT64,
    description="Customer identifier",
)

# Define the source of our features
customer_stats_source = FileSource(
    path="data/processed/customer_features",
    timestamp_field="dt",
)

# Define feature view
customer_stats_view = FeatureView(
    name="customer_statistics",
    entities=["customer"],
    ttl=timedelta(days=1),
    features=[
        Feature(name="total_spend_30d", dtype=Float32),
        Feature(name="avg_transaction_value_30d", dtype=Float32),
        Feature(name="transaction_count_30d", dtype=Int64),
        Feature(name="days_since_last_transaction", dtype=Int64),
    ],
    online=True,
    source=customer_stats_source,
    tags={"team": "ml_team"},
    description="Customer transaction statistics for the last 30 days",
)