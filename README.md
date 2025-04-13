# Feature Store Pipeline

An end-to-end feature engineering pipeline with automated feature extraction, transformation, and storage. This pipeline leverages Apache Spark for efficient processing of large datasets and ensures point-in-time correctness for ML models.

## Prerequisites

Before running the pipeline, ensure you have the following installed:

1. Python 3.8+ (with pip)
2. Java 11+ (required for Apache Spark)
3. Redis Server
4. Apache Spark 3.4+

## Architecture

- **Apache Spark**: Distributed data processing for feature transformations
- **Feast**: Feature store for managing and serving features
- **Airflow**: Workflow orchestration for automated pipeline runs
- **Redis**: Online feature serving store

## Project Structure

```
feature-store-pipeline/
├── airflow/
│   ├── dags/
│   └── plugins/
├── feast/
│   ├── feature_repo/
│   │   ├── feature_definitions/
│   │   ├── feature_services/
│   │   └── data_sources/
│   └── feature_store.yaml
├── spark/
│   └── transformations/
├── data/
│   ├── raw/
│   └── processed/
├── scripts/
│   └── setup.sh
└── requirements.txt
```

## Setup

1. Install prerequisites (Java, Redis, Apache Spark)

2. Install Python dependencies:
```bash
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

3. Configure environment:
```bash
./scripts/setup.sh
```

## Components

### Feature Transformations (Spark)
- Raw data processing
- Feature engineering logic
- Data validation and quality checks

### Feature Store (Feast)
- Feature registry and definitions
- Feature serving layer
- Point-in-time correct feature retrieval

### Pipeline Orchestration (Airflow)
- Automated feature computation
- Data freshness monitoring
- Pipeline scheduling and monitoring

### Online Store (Redis)
- Low-latency feature serving
- Real-time feature updates
- Feature vector assembly

## Usage

1. Define features in Feast:
```python
from feast import Entity, Feature, FeatureView, ValueType

# Define entities and features
```

2. Run feature pipeline:
```bash
airflow dags trigger feature_pipeline
```

3. Retrieve features:
```python
from feast import FeatureStore

store = FeatureStore("feast/feature_repo")
features = store.get_online_features(...)
```