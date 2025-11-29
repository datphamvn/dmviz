# Team Guide

## Quick Reference

### Pipeline Flow & I/O Contracts

```
[Datasets] or [Ingestion] → [Processing] → [Storage] → [Models] → [Viz]
   name        str|Path      DataFrame     DataFrame   arrays     arrays
     ↓            ↓             ↓              ↓          ↓          ↓
 DataFrame    DataFrame     DataFrame        bool      arrays     Figure
```

---

## Ownership

| Module | Owner | Files |
|--------|-------|-------|
| `datasets/` | All (shared) | `__init__.py`, `catalog.yaml` |
| `ingestion/` | Data Engineering | `base.py`, `connectors.py`, `streaming.py` |
| `processing/` | Data Engineering | `base.py`, `cleaners.py`, `transformers.py`, `features.py` |
| `storage/` | Platform | `base.py`, `file.py`, `warehouse.py` |
| `models/` | ML/Statistics | `base.py` + `regression.py`, `classification.py`, `em.py`, `mle.py` (planned) |
| `viz/` | Analytics | `base.py` + `eda.py`, `model.py`, `cluster.py` (planned) |
| `core/` | All (shared) | `base.py`, `registry.py` |
| `utils/` | All (shared) | `helpers.py` |

---

## I/O Contracts by Module

### 0. Datasets

```
INPUT:  str (dataset name)
OUTPUT: pd.DataFrame
```

| Function | Input | Output | Notes |
|----------|-------|--------|-------|
| `list_datasets()` | — | `list[str]` | Available dataset names |
| `load_dataset(name)` | dataset name | DataFrame | Load by name |
| `get_catalog()` | — | `dict` | Full catalog metadata |

### 1. Ingestion

```
INPUT:  str | Path | dict (source location/config)
OUTPUT: pd.DataFrame
```

| Component | Input | Output | Config |
|-----------|-------|--------|--------|
| `CSVIngester` | file path | DataFrame | delimiter, encoding |
| `JSONIngester` | file path | DataFrame | orient, lines |
| `ParquetIngester` | file path | DataFrame | — |
| `APIIngester` | URL | DataFrame | headers, params, json_path |
| `KafkaConsumer` | config dict | Iterator[DataFrame] | bootstrap_servers, topic |
| `auto_ingest()` | file path | DataFrame | Auto-detects format |

### 2. Processing

```
INPUT:  pd.DataFrame
OUTPUT: pd.DataFrame
```

| Component | Input | Output | State (after fit) |
|-----------|-------|--------|-------------------|
| `MissingHandler` | DataFrame | DataFrame | fill_values_ |
| `OutlierHandler` | DataFrame | DataFrame | bounds_ |
| `Normalizer` | DataFrame | DataFrame | stats_ (mean, std) |
| `Encoder` | DataFrame | DataFrame | mappings_ |
| `FeatureEngineer` | DataFrame | DataFrame | operations config |
| `ColumnSelector` | DataFrame | DataFrame | columns, mode |
| `ProcessorChain` | DataFrame | DataFrame | (chains processors) |

### 3. Storage

```
save(): pd.DataFrame + name → bool | Path
read(): name → pd.DataFrame
```

| Component | save() | read() | Notes |
|-----------|--------|--------|-------|
| `DataLake` | `(df, name, layer)` → Path | `(name, layer)` → DataFrame | bronze/silver/gold layers |
| `ParquetStorage` | `df` → bool | `path` → DataFrame | Columnar format |
| `CSVStorage` | `df` → bool | `path` → DataFrame | Plain text format |
| `WarehouseStorage` | `df` → bool | `table\|query` → DataFrame | SQL databases |
| `Layer` (type) | — | — | `"bronze"` \| `"silver"` \| `"gold"` |

### 4. Models

```
fit():     X, y → self
predict(): X → np.ndarray
```

| Model | fit input | predict output | result_ metrics |
|-------|-----------|----------------|-----------------|
| `LinearRegressor` | `(n,p), (n,)` | `(n,)` | r2, mse, rmse, mae |
| `LogisticClassifier` | `(n,p), (n,)` | `(n,)` labels | accuracy, log_loss |
| `EMClustering` | `(n,p)` | `(n,)` labels | log_likelihood, bic |

### 5. Visualization

```
INPUT:  arrays, DataFrame, or model outputs
OUTPUT: Figure (has .save(), .show(), .close())
```

| Function | Input | Output | Category |
|----------|-------|--------|----------|
| `distribution_plot` | 1D array | Figure (hist + kde) | EDA |
| `histogram` | 1D array | Figure | EDA |
| `boxplot` | DataFrame | Figure | EDA |
| `correlation_heatmap` | DataFrame | Figure (heatmap) | EDA |
| `regression_diagnostics` | y_true, y_pred | Figure (4 panels) | Model |
| `roc_curve` | y_true, y_score | Figure | Model |
| `confusion_matrix` | y_true, y_pred | Figure | Model |
| `learning_curve` | train/val scores | Figure | Model |
| `cluster_scatter` | X, labels | Figure | Cluster |
| `gmm_contours` | X, labels, means, covs | Figure | Cluster |
| `elbow_plot` | k values, scores | Figure | Cluster |

---

## Git Workflow

### Branch Strategy

```
main              ← production-ready code
  └── dev         ← integration branch
       ├── feat/ingestion-kafka     ← feature branches
       ├── feat/models-logistic
       └── fix/processing-nan
```

### Branch Naming

```
feat/<module>-<feature>    # New features
fix/<module>-<issue>       # Bug fixes
refactor/<module>-<what>   # Code improvements
docs/<what>                # Documentation only
```

### Workflow

```bash
# 1. Create feature branch from dev
git checkout dev
git pull origin dev
git checkout -b feat/ingestion-excel

# 2. Work on your module
# ... edit files in dmviz/ingestion/ ...

# 3. Commit with conventional messages
git add dmviz/ingestion/
git commit -m "feat(ingestion): add ExcelIngester"

# 4. Push and create PR to dev
git push -u origin feat/ingestion-excel
# Create PR: feat/ingestion-excel → dev

# 5. After review, merge to dev
# 6. Periodically: dev → main (releases)
```

### Commit Messages

```
<type>(<module>): <description>

Types: feat, fix, refactor, docs, test, chore
Module: ingestion, processing, storage, models, viz, core
```

Examples:
```
feat(models): implement gradient descent solver
fix(processing): handle empty DataFrame in MissingHandler
refactor(storage): simplify DataLake path handling
docs(viz): add examples for regression_diagnostics
```

---

## Adding New Components

### 1. Create the class

```python
# dmviz/<module>/your_file.py
from dmviz.core.registry import Registry
from dmviz.<module>.base import Base<Type>

@Registry.register("your_component", category="<module>")
class YourComponent(Base<Type>):
    """
    One-line description.
    
    Config: param1, param2
    INPUT:  <input type>
    OUTPUT: <output type>
    """
    
    name = "your_component"
    
    def _run(self, input_data):
        raise NotImplementedError("TODO: Implement")
```

### 2. Export in `__init__.py`

```python
# dmviz/<module>/__init__.py
from dmviz.<module>.your_file import YourComponent

__all__ = [..., "YourComponent"]
```

### 3. Test independently

```python
# tests/<module>/test_your_component.py
def test_your_component():
    component = YourComponent(config)
    result = component.execute(input_data)
    assert result.success
```

---

## Working Independently

Each module can be developed in isolation. Follow these rules:

### DO
- Use the defined I/O contracts
- Write tests for your module
- Document config options and I/O types
- Use `NotImplementedError` for TODOs

### DON'T
- Modify other teams' modules
- Change base class interfaces without discussion
- Add dependencies without team review
- Commit directly to `main` or `dev`

### Coordination Points

| Scenario | Action |
|----------|--------|
| Need new base class method | PR to `core/`, notify all teams |
| Need data from another stage | Use existing I/O contract |
| Found bug in shared code | Create issue, assign to owner |
| Proposing interface change | RFC in GitHub Discussion |

---

## Project Structure

```
dmviz/
├── __init__.py           # Package root, exports Registry, Pipeline
├── core/                 # Shared abstractions (DO NOT MODIFY without team review)
│   ├── base.py          # BaseComponent, Pipeline, StageResult
│   └── registry.py      # Component registry
├── datasets/            # Shared - demo datasets
│   ├── __init__.py      # load_dataset, list_datasets, get_catalog
│   └── catalog.yaml     # Dataset metadata (sources, features, targets)
├── ingestion/           # Data Engineering Team
│   ├── base.py          # BaseIngester, BatchIngester
│   ├── connectors.py    # CSV, JSON, API, Parquet ingesters + auto_ingest
│   └── streaming.py     # Kafka, streaming sources
├── processing/          # Data Engineering Team
│   ├── base.py          # BaseProcessor, ProcessorChain
│   ├── cleaners.py      # MissingHandler, OutlierHandler
│   ├── transformers.py  # Normalizer, Encoder
│   └── features.py      # FeatureEngineer, ColumnSelector
├── storage/             # Platform Team
│   ├── base.py          # BaseStorage, DataLake, Layer
│   ├── file.py          # ParquetStorage, CSVStorage
│   └── warehouse.py     # WarehouseStorage (SQL databases)
├── models/              # ML/Statistics Team
│   ├── base.py          # BaseModel, ModelResult
│   ├── regression.py    # LinearRegressor (planned)
│   ├── classification.py # LogisticClassifier (planned)
│   ├── em.py            # EMClustering/GMM (planned)
│   └── mle.py           # MLEEstimator (planned)
├── viz/                 # Analytics Team
│   ├── base.py          # Figure, PlotStyle, Plotter
│   ├── eda.py           # distribution_plot, histogram, boxplot, correlation_heatmap (planned)
│   ├── model.py         # regression_diagnostics, roc_curve, confusion_matrix (planned)
│   └── cluster.py       # cluster_scatter, gmm_contours, elbow_plot (planned)
└── utils/               # Shared utilities
    └── helpers.py       # load_config, timer, validate_dataframe
```

> **Note:** Files marked `(planned)` are defined in `__init__.py` but not yet implemented.

---

## Docker

### Build & Run

```bash
# Build image
docker compose -f docker/docker-compose.yml build

# Start container (interactive)
docker compose -f docker/docker-compose.yml up -d
docker compose -f docker/docker-compose.yml exec dmviz python

# Run script
docker compose -f docker/docker-compose.yml exec dmviz python -c "from dmviz.datasets import list_datasets; print(list_datasets())"

# Stop
docker compose -f docker/docker-compose.yml down
```

### Files

| File | Purpose |
|------|---------|
| `docker/Dockerfile` | Python 3.11 image with dmviz installed |
| `docker/docker-compose.yml` | Development setup with volume mounts |
| `.dockerignore` | Excludes .git, __pycache__, docs |

---

## Quick Start for Each Team

### Using Built-in Datasets

```python
# Quick access to demo datasets
from dmviz.datasets import load_dataset, list_datasets

print(list_datasets())  # ['california_housing', ...]
df = load_dataset("california_housing")
```

### Data Engineering (ingestion + processing)

```python
# Your domain: load and clean data
from dmviz.ingestion import CSVIngester, auto_ingest
from dmviz.processing import ProcessorChain, MissingHandler, ColumnSelector

# Option 1: Auto-detect file format
df = auto_ingest("data.csv")

# Option 2: Explicit ingester
ingester = CSVIngester({"delimiter": ","})
result = ingester.execute("data.csv")  # Returns StageResult[DataFrame]

# Process
chain = ProcessorChain([
    MissingHandler(),
    ColumnSelector({"columns": ["col1", "col2"], "mode": "keep"})
])
clean_df = chain.process(result.data)
```

### Platform (storage)

```python
# Your domain: persist data across layers
from dmviz.storage import DataLake, ParquetStorage, CSVStorage

# DataLake with bronze/silver/gold layers (Medallion Architecture)
lake = DataLake("./data")
lake.save(df, "customers", layer="bronze")
df = lake.read("customers", layer="silver")

# Direct file storage
parquet = ParquetStorage({"path": "./data/output.parquet"})
parquet.save(df)
```

### ML/Statistics (models)

```python
# Your domain: train and predict
from dmviz.models import LinearRegressor

# Implement fit/predict methods
model = LinearRegressor(solver="analytic")
model.fit(X_train, y_train)
predictions = model.predict(X_test)
print(model.result_.metrics)
```

### Analytics (viz)

```python
# Your domain: visualize results
from dmviz.viz import (
    regression_diagnostics, correlation_heatmap,
    distribution_plot, cluster_scatter
)

# EDA plots
fig = correlation_heatmap(df)
fig = distribution_plot(df["column"])

# Model diagnostics
fig = regression_diagnostics(y_true, y_pred)
fig.save("diagnostics.png")

# Clustering
fig = cluster_scatter(X, labels)
```
