# OpenWeather AWS ML Pipeline

Enterprise-grade weather data collection and ML prediction system with end-to-end data engineering, MLOps, and full-stack development.

## 🎯 Project Overview

Automated weather data pipeline collecting 6 years of historical data (2020-2025) across 5 NYC locations,and one NY state location with planned ML forecasting and LLM-powered explanations. Built with production-ready practices: type safety, validation, CI/CD, and infrastructure as code.

## 🏗️ Architecture

**Data Collection Layer:**
- AWS Lambda functions for serverless data ingestion (Python 3.11)
- EventBridge scheduling for automated daily collection (950 API calls/day limit handling)
- DynamoDB-based job queue with progress tracking and retry logic
- S3 partitioned storage (year/month/day/zipcode structure)

**Infrastructure:**
- Terraform IaC with lifecycle management (persistent vs ephemeral resources)
- GitHub Actions CI/CD with OIDC authentication (no permanent credentials)
- Lambda layers for dependency management (numpy 2.x, pandas, pydantic)

**Code Quality:**
- Type hints with mypy static analysis
- Pydantic models for data validation at boundaries
- Centralized logging (CloudWatch integration)
- Black formatter + flake8 linting (88 char line length)

**Upcoming:**
- LSTM/XGBoost models for temperature prediction
- AWS Bedrock (Claude) for natural language forecast explanations
- React + API Gateway frontend
- Feature store (DynamoDB) for ML serving

## 🛠️ Tech Stack

**Cloud & Infrastructure:**
- AWS: Lambda, S3, DynamoDB, EventBridge, CloudWatch, SSM Parameter Store
- Terraform (IaC with state management)
- GitHub Actions (OIDC-based deployment)

**Backend:**
- Python 3.11 (type-safe, validated)
- Pydantic v2 (schema validation)
- boto3 (AWS SDK)
- Structured logging

**Development:**
- pyproject.toml package management
- mypy, black, flake8
- Modular architecture (config manager, operations classes)

**Planned Changes:**
- PyTorch/scikit-learn (ML models)
- AWS Bedrock (LLM integration)
- React + TypeScript (frontend)
- Step Functions (orchestration)

## 📊 Key Features

✅ **Rate-Limited Historical Collection**: Queue-based system processing 13,152+ data points (6 locations × 6 years) within API constraints

✅ **Production Patterns**: Singleton config manager, validated DynamoDB operations, idempotent Lambda handlers

✅ **Type Safety**: Full type hints, Pydantic validation at all I/O boundaries, mypy-checked

✅ **CI/CD**: Automated testing, linting, secure deployment via OIDC, Terraform state management

✅ **Cost Optimized**: Serverless architecture, lifecycle-based resource management, <$30/month for full pipeline

## 🚀 Local Development

```bash
# Install package (editable mode)
pip install -e ".[dev]"

# Run code quality checks
black src/ --check
flake8 src/
mypy src/

# Run data collection (local)
python -m openweather_pipeline.weather_data_collector
```

## 📁 Project Structure

```
├── src/openweather_pipeline/          # Main package
│   ├── weather_data_collector.py      # Core collection logic
│   ├── config_manager.py              # Singleton config (SSM + YAML)
│   ├── dynamodb_operations.py         # Type-safe DB wrapper
│   ├── s3_operations.py               # S3 client with validation
│   └── models/                        # Pydantic schemas
├── terraform/                         # Infrastructure definitions
├── .github/workflows/                 # CI/CD pipelines
├── config/                            # Configuration files
└── pyproject.toml                     # Package + tool configuration
```

## 📈 Roadmap

- [✅] Complete historical data collection (6 years × 5 locations)
- [  ] Time-series feature engineering pipeline
- [  ] LSTM temperature prediction model
- [  ] AWS Bedrock LLM integration for explanations
- [  ] REST API with API Gateway
- [  ] React dashboard with prediction visualization
- [  ] Unit tests with moto (AWS mocking)

