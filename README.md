# Linear Models - Data Management & Visualization

A modular Python repository for end-to-end data pipelines. Each team member owns a stage.

---

## Pipeline

```
[Datasets]  ─┐
             ├→ [Processing] → [Storage] → [Models] → [Viz]
[Ingestion] ─┘
```

## Ownership

| Module | Owner | Contract |
|--------|-------|----------|
| `datasets/` | Shared | `name → DataFrame` |
| `src/ingestion/` | Data Engineering | `str\|Path → DataFrame` |
| `src/processing/` | Data Engineering | `DataFrame → DataFrame` |
| `src/storage/` | Platform | `DataFrame ↔ Storage` |
| `src/models/` | ML/Statistics | `fit(X,y) → predict(X)` |
| `src/viz/` | Analytics | `data → Figure` |

## Quick Start

```bash
pip install -e .
```

### Docker

```bash
docker compose -f docker/docker-compose.yml up -d
docker compose -f docker/docker-compose.yml exec dmviz python
```

```python
from dmviz.datasets import load_dataset, list_datasets
from dmviz.src.ingestion import auto_ingest
from dmviz.src.processing import ProcessorChain, MissingHandler
from dmviz.src.models import LinearRegressor
from dmviz.src.viz import regression_diagnostics

# Load data (choose one)
df = load_dataset("california_housing")  # Built-in datasets
df = auto_ingest("data.csv")             # From file (auto-detect format)

# Clean → Model → Visualize
clean_df = ProcessorChain([MissingHandler()]).process(df)
model = LinearRegressor().fit(clean_df[["x"]], clean_df["y"])
fig = regression_diagnostics(clean_df["y"], model.predict(clean_df[["x"]]))
```

## Documentation

- **[Team Guide](docs/guide.md)** - I/O contracts, git workflow, team collaboration
