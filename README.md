# Linear Models - Data Management & Visualization

Modular Python framework for end-to-end data pipelines. Each team owns a stage.

## Pipeline

```
[Datasets/Ingestion] → [Processing] → [Storage] → [Models] → [Viz]
```

## Structure

```
dmviz/
├── src/                    # Core library
│   ├── datasets/           # Demo datasets & catalog
│   ├── ingestion/          # Data connectors
│   ├── processing/         # Cleaners & transformers
│   ├── storage/            # File & warehouse backends
│   ├── models/             # ML models
│   └── viz/                # Visualization
├── pipelines/airflow/      # Airflow DAGs
├── services/mock_api/      # Mock API for testing
└── docker/                 # Container setup
```

## Ownership

| Module | Owner | Contract |
|--------|-------|----------|
| `src/datasets/` | Shared | `name → DataFrame` |
| `src/ingestion/` | Data Engineering | `str\|Path → DataFrame` |
| `src/processing/` | Data Engineering | `DataFrame → DataFrame` |
| `src/storage/` | Platform | `DataFrame ↔ Storage` |
| `src/models/` | ML/Statistics | `fit(X,y) → predict(X)` |
| `src/viz/` | Analytics | `data → Figure` |

## Quick Start

```bash
pip install -e .
```

```python
from dmviz.src.datasets import load_dataset
from dmviz.src.ingestion import auto_ingest
from dmviz.src.processing import ProcessorChain, MissingHandler
from dmviz.src.models import LinearRegressor
from dmviz.src.viz import regression_diagnostics

# Load data
df = load_dataset("california_housing")  # or: auto_ingest("data.csv")

# Clean → Model → Visualize
clean_df = ProcessorChain([MissingHandler()]).process(df)
model = LinearRegressor().fit(clean_df[["x"]], clean_df["y"])
fig = regression_diagnostics(clean_df["y"], model.predict(clean_df[["x"]]))
```

### Docker

```bash
docker compose -f docker/docker-compose.yml up -d
docker compose -f docker/docker-compose.yml exec dmviz python
```

## Docs

- **[Team Guide](docs/guide.md)** - I/O contracts, git workflow, project structure
