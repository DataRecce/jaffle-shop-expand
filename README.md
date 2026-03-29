# Jaffle Shop Expanded

A medium-sized dbt project (~1,000 models) built on top of [dbt-labs/jaffle-shop](https://github.com/dbt-labs/jaffle-shop). Designed as a realistic mock data warehouse for testing dbt tools, PR review workflows, and data platform capabilities.

## Quick Start

```bash
# Install dependencies
uv run --with dbt-duckdb dbt deps --profiles-dir .

# Load seed data and build everything
uv run --with dbt-duckdb dbt seed --profiles-dir . --target duckdb --vars 'load_source_data: true'
uv run --with dbt-duckdb dbt build --full-refresh --profiles-dir . --target duckdb --vars 'load_source_data: true'

# Generate and serve docs
uv run --with dbt-duckdb dbt docs generate --profiles-dir . --target duckdb
uv run --with dbt-duckdb dbt docs serve --profiles-dir .
```

## Project Stats

| Resource | Count |
|----------|-------|
| SQL models | 1,058 |
| Schema tests | ~800 |
| Seeds (CSV) | 58 |
| Macros | 22 |
| Snapshots | 2 |
| Analyses | 3 |
| Data tests | 3 |
| Exposures | 6 |
| Groups | 7 |
| Custom generic tests | 4 |
| Docs blocks | 11 |
| Source tables | 58 |

## Business Domains

The project models a coffee chain with five business domains, each with staging, intermediate, and mart layers:

| Domain | Description | Sources |
|--------|-------------|---------|
| **Core (ecom)** | Orders, customers, products, stores, supplies | 6 tables |
| **Finance** | Invoices, refunds, payments, gift cards, budgets, expenses | 10 tables |
| **Supply Chain** | Suppliers, purchase orders, inventory, warehouses, waste | 10 tables |
| **Marketing** | Campaigns, coupons, loyalty program, email, social, referrals | 10 tables |
| **HR & Operations** | Employees, shifts, payroll, training, equipment, maintenance | 12 tables |
| **Product & Menu** | Recipes, ingredients, menu items, nutrition, pricing, reviews | 10 tables |

## Model Architecture

```
seeds/ (58 CSVs)
  jaffle-data/        Original ecom data (935 customers, 62K orders)
  finance-data/       10 seed files
  supply-chain-data/  10 seed files
  marketing-data/     10 seed files
  hr-ops-data/        12 seed files
  product-data/       10 seed files

models/
  staging/            88 models (6 domain subdirectories + derived)
  intermediate/       194 models (13 subdirectories)
  marts/              714 models (28 subdirectories)
  utilities/          4 models (date spine, fiscal periods, calendars)
```

### Mart Subdirectories

| Directory | Prefix | Models | Description |
|-----------|--------|--------|-------------|
| `finance/` | `fct_`, `dim_`, `rpt_` | 29 | Core finance facts, dims, reports |
| `supply_chain/` | `fct_`, `dim_`, `rpt_` | 29 | Supply chain analytics |
| `marketing/` | `fct_`, `dim_`, `rpt_` | 29 | Marketing and loyalty |
| `hr_ops/` | `fct_`, `dim_`, `rpt_` | 29 | HR and operations |
| `product/` | `fct_`, `dim_`, `rpt_` | 29 | Product and menu |
| `cross_domain/` | `dim_`, `rpt_`, `int_` | 21 | Customer 360, store economics |
| `metrics/` | `met_` | 18 | Pre-aggregated time-series metrics |
| `scoring/` | `scr_` | 6 | Entity health/risk scores (0-100) |
| `executive/` | `exec_` | 6 | C-level dashboards |
| `cohorts/` | `coh_` | 8 | Cohort retention analysis |
| `funnels/` | `fnl_` | 6 | Conversion funnels |
| `comparisons/` | `cmp_` | 8 | Period/entity comparisons |
| `ml_features/` | `ml_` | 6 | ML feature store tables |
| `kpis/` | `kpi_` | 35 | Standalone KPI models |
| `trends/` | `trend_` | 40 | Trend analysis with moving averages |
| `rankings/` | `rank_` | 30 | Entity league tables |
| `alerts/` | `alert_` | 30 | Threshold-based monitoring |
| `distributions/` | `dist_` | 25 | Percentile/histogram models |
| `summaries/` | `sum_` | 30 | Pre-computed aggregates |
| `geo/` | `geo_` | 25 | Geographic/location analysis |
| `role_views/` | `view_` | 30 | Persona-specific views (CFO, COO, etc.) |
| `incremental/` | `inc_` | 15 | Incremental materializations |
| `reverse_etl/` | `rev_etl_` | 10 | Reverse ETL staging |
| `wide_tables/` | `wide_` | 20 | Denormalized BI tables |
| `mega_wide/` | `mega_wide_` | 3 | 80+ column master tables |
| `advanced_sql/` | `adv_` | 30 | Advanced SQL patterns |
| `narrow/` | `narrow_` | 25 | Single-column/metric models |
| `analytics/` | `rpt_` | 30 | Cross-domain analytics |
| `data_quality/` | `dq_` | 8 | Data quality checks |
| `*_advanced/` | `fin_`, `sc_`, etc. | 120 | Deep domain analytics |
| `period_comparison/` | `poc_` | 35 | Period-over-period |

## Tags

Models are tagged for selective execution:

```bash
# By domain
dbt run --select tag:domain:finance
dbt run --select tag:domain:marketing

# By layer
dbt run --select tag:layer:staging
dbt run --select tag:layer:marts

# By type
dbt run --select tag:type:metric
dbt run --select tag:type:kpi
dbt run --select tag:type:alert

# By criticality
dbt run --select tag:criticality:high

# By audience
dbt run --select tag:audience:cfo
dbt run --select tag:audience:data_team

# By cadence
dbt run --select tag:cadence:daily
```

## Column Variety

Models range from 1 column (narrow metrics) to 80+ columns (mega-wide master tables):

| Column Range | Models |
|-------------|--------|
| 1-3 columns | ~60 |
| 4-8 columns | ~520 |
| 9-15 columns | ~410 |
| 16-30 columns | ~55 |
| 31-80+ columns | ~13 |

## Advanced SQL Patterns

The `advanced_sql/` directory demonstrates 30 SQL techniques:

- Recursive CTEs (org hierarchy, referral trees)
- GROUPING SETS / CUBE / ROLLUP
- Window frame tricks (excluding current row, cumulative with reset)
- Gap-and-island detection
- Correlated subqueries
- Array operations
- NOT EXISTS patterns
- Self-joins for graph/network analysis
- Complex CASE business rule engines

## Reusable Macros

| Macro | Description |
|-------|-------------|
| `rolling_average` | Configurable rolling window average |
| `safe_divide` | Division with zero/null safety |
| `growth_rate` | Period-over-period growth percentage |
| `bucket_values` | Numeric bucketing into labeled ranges |
| `percentile_score` | Ntile-based percentile scoring |
| `flag_outlier` | Standard deviation-based outlier detection |
| `weighted_score` | Weighted composite scores |
| `classify_trend` | Trend direction classification |
| `pivot_column` | Row-to-column pivoting |
| `unpivot_columns` | Column-to-row melting |
| `running_total` | Cumulative sum |
| `deduplicate` | Row-number deduplication |
| `surrogate_key_hash` | MD5 hash key generation |
| `day_of_week_number` | Cross-database day-of-week extraction |

## dbt Features Used

- Models (view, table, incremental)
- Seeds with schema routing
- Sources with freshness checks
- Schema tests (unique, not_null, accepted_values, relationships)
- Custom generic tests (positive_value, not_in_future, valid_percentage, referential_integrity)
- Data tests (custom SQL assertions)
- Snapshots (SCD Type 2)
- Analyses (ad-hoc compiled queries)
- Exposures (dashboards, apps, ML models)
- Groups (team ownership)
- Docs blocks (business concept documentation)
- Meta properties (PII flags, ownership)
- Model contracts (enforced column types)
- Tags (6 categories, 55+ unique tags)
- Adapter dispatch (DuckDB + Snowflake compatible)
- Semantic models and metrics (from original jaffle-shop)
- Unit tests (from original jaffle-shop)

## Database Support

| Target | Status | Profile |
|--------|--------|---------|
| DuckDB | Full build passes (1918/1918) | `--target duckdb` (default) |
| DuckDB (Recce base) | Same file, `base` schema | `--target duckdb-base` |
| DuckDB (Recce current) | Same file, `current` schema | `--target duckdb-current` |
| Snowflake | SQL compatible, needs credentials | `--target snowflake` |

## Recce Setup

[Recce](https://github.com/DataRecce/recce) compares two dbt environments to catch data impact during PR review. This project includes two DuckDB targets (`duckdb-base` and `duckdb-current`) that share the same database file but use separate schemas.

```bash
# 1. Build the base environment (e.g., from the main branch)
uv run --with dbt-duckdb dbt seed --profiles-dir . --target duckdb-base --vars 'load_source_data: true'
uv run --with dbt-duckdb dbt build --full-refresh --profiles-dir . --target duckdb-base --vars 'load_source_data: true'

# 2. Switch to your feature branch, then build the current environment
uv run --with dbt-duckdb dbt seed --profiles-dir . --target duckdb-current --vars 'load_source_data: true'
uv run --with dbt-duckdb dbt build --full-refresh --profiles-dir . --target duckdb-current --vars 'load_source_data: true'

# 3. Start Recce server
uv run --with recce recce server --target-base-path target-base
```

Pre-built base artifacts (`manifest.json`, `catalog.json`) are included in `target-base/` so you can skip step 1 if you just want to explore.

## Intentional SQL Mistakes

20 subtle logic errors are planted across mart models for testing PR review tools. These are syntactically valid but produce incorrect results:

- Wrong join types (INNER vs LEFT)
- Off-by-one date filters
- Wrong aggregation (COUNT vs COUNT DISTINCT)
- Double-counting from fan-out joins
- Hardcoded values that go stale
- Wrong column in calculations

The answer key is available for validation.

## Project Structure

```
jaffle-shop-expand/
├── analyses/              3 ad-hoc analysis queries
├── data-tests/            3 custom data test assertions
├── dbt_project.yml        Project config with tags
├── macros/                22 reusable macros
│   └── tests/             4 custom generic tests
├── models/
│   ├── _groups.yml        Team ownership groups
│   ├── docs.md            Business concept documentation
│   ├── staging/           88 staging models (6 domains + derived)
│   ├── intermediate/      194 intermediate models
│   ├── marts/             714 mart models (28 subdirectories)
│   └── utilities/         4 utility models
├── packages.yml           dbt_utils, dbt_date, dbt_audit_helper
├── profiles.yml           DuckDB (default) + Snowflake targets
├── seeds/                 58 CSV seed files (6 directories)
└── snapshots/             2 snapshot definitions
```

## Origin

Extended from [dbt-labs/jaffle-shop](https://github.com/dbt-labs/jaffle-shop) (v3.0.0). The original 14 models and 6 seeds are preserved; all extensions are additive.
