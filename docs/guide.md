# Team Guide

## Pipeline Flow & I/O Contracts

```
[Datasets/Ingestion] → [Processing] → [Storage] → [Models] → [Viz]
   name | str|Path      DataFrame     DataFrame   arrays     arrays
         ↓                 ↓              ↓          ↓          ↓
      DataFrame         DataFrame       bool      arrays     Figure
```

## Ownership

| Module | Owner | Files |
|--------|-------|-------|
| `src/datasets/` | All (shared) | `__init__.py`, `catalog.yaml` |
| `src/ingestion/` | Data Engineering | `base.py`, `connectors.py`, `streaming.py` |
| `src/processing/` | Data Engineering | `base.py`, `cleaners.py`, `transformers.py`, `features.py` |
| `src/storage/` | Platform | `base.py`, `file.py`, `warehouse.py` |
| `src/models/` | ML/Statistics | `base.py` + `regression.py`, `classification.py`, `em.py`, `mle.py` (planned) |
| `src/viz/` | Analytics | `base.py` + `eda.py`, `model.py`, `cluster.py` (planned) |
| `src/core/` | All (shared) | `base.py`, `registry.py` |
| `src/utils/` | All (shared) | `helpers.py` |

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
# ... edit files in dmviz/src/ingestion/ ...

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

```python
# 1. Create: src/<module>/your_file.py
from dmviz.src.core.registry import Registry
from dmviz.src.<module>.base import Base<Type>

@Registry.register("your_component", category="<module>")
class YourComponent(Base<Type>):
    name = "your_component"
    
    def _run(self, input_data):
        raise NotImplementedError("TODO")

# 2. Export: src/<module>/__init__.py
from dmviz.src.<module>.your_file import YourComponent
__all__ = [..., "YourComponent"]
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
| Need new base class method | PR to `src/core/`, notify all teams |
| Need data from another stage | Use existing I/O contract |
| Found bug in shared code | Create issue, assign to owner |
| Proposing interface change | RFC in GitHub Discussion |

---

## Project Structure

```
dmviz/
├── __init__.py              # Package root
├── src/                     # Core library
│   ├── core/                # Shared abstractions (review before modifying)
│   │   ├── base.py          # BaseComponent, Pipeline, StageResult
│   │   └── registry.py      # Component registry
│   ├── datasets/            # Demo datasets
│   │   ├── __init__.py      # load_dataset, list_datasets, get_catalog
│   │   └── catalog.yaml     # Dataset metadata
│   ├── ingestion/           # Data Engineering
│   │   ├── base.py          # BaseIngester, BatchIngester
│   │   ├── connectors.py    # CSV, JSON, API, Parquet + auto_ingest
│   │   └── streaming.py     # Kafka, streaming sources
│   ├── processing/          # Data Engineering
│   │   ├── base.py          # BaseProcessor, ProcessorChain
│   │   ├── cleaners.py      # MissingHandler, OutlierHandler
│   │   ├── transformers.py  # Normalizer, Encoder
│   │   └── features.py      # FeatureEngineer, ColumnSelector
│   ├── storage/             # Platform
│   │   ├── base.py          # BaseStorage, DataLake, Layer
│   │   ├── file.py          # ParquetStorage, CSVStorage
│   │   └── warehouse.py     # WarehouseStorage (SQL)
│   ├── models/              # ML/Statistics
│   │   └── base.py          # BaseModel, ModelResult
│   ├── viz/                 # Analytics
│   │   └── base.py          # Figure, PlotStyle, Plotter
│   └── utils/               # Shared utilities
│       └── helpers.py       # load_config, timer, validate_dataframe
├── pipelines/               # Workflow orchestration
│   └── airflow/             # Airflow DAGs
├── services/                # External services
│   └── mock_api/            # FastAPI mock data service
├── docker/                  # Container setup
└── docs/                    # Documentation
```

---

## Docker

```bash
docker compose -f docker/docker-compose.yml build
docker compose -f docker/docker-compose.yml up -d
docker compose -f docker/docker-compose.yml exec dmviz python
docker compose -f docker/docker-compose.yml down
```

---

## Quick Start by Team

### Datasets

```python
from dmviz.src.datasets import load_dataset, list_datasets

print(list_datasets())  # ['california_housing', ...]
df = load_dataset("california_housing")
```

### Data Engineering (ingestion + processing)

```python
from dmviz.src.ingestion import CSVIngester, auto_ingest
from dmviz.src.processing import ProcessorChain, MissingHandler, ColumnSelector

df = auto_ingest("data.csv")  # Auto-detect format

chain = ProcessorChain([
    MissingHandler(),
    ColumnSelector({"columns": ["col1", "col2"], "mode": "keep"})
])
clean_df = chain.process(df)
```

### Platform (storage)

```python
from dmviz.src.storage import DataLake, ParquetStorage

lake = DataLake("./data")
lake.save(df, "customers", layer="bronze")
df = lake.read("customers", layer="silver")
```

### ML/Statistics (models)

```python
from dmviz.src.models import LinearRegressor

model = LinearRegressor(solver="analytic")
model.fit(X_train, y_train)
predictions = model.predict(X_test)
```

### Analytics (viz)

```python
from dmviz.src.viz import regression_diagnostics, correlation_heatmap

fig = correlation_heatmap(df)
fig = regression_diagnostics(y_true, y_pred)
fig.save("diagnostics.png")
```
