# Healthcare Data Warehouse Bruin Pipeline

This Bruin pipeline builds a BigQuery healthcare warehouse with bronze, silver, and gold layers.

## Structure

```text
pipeline/
  pipeline.yml
  assets/
    bronze/   raw batch seeds and streaming vitals ingestion
    silver/   cleaned, typed, PII-free source models
    gold/     dimensional warehouse tables
```

## Sources

- Batch CSV files come from `data/` in this repository and are loaded with Bruin `bq.seed` assets.
- Streaming vitals are ingested from `gs://de-zoomcamp-493207-healthcare-lake/healthcare_vitals/` with a Bruin `ingestr` asset.

PII fields such as `ssn`, `passport`, `drivers`, and phone numbers are only present in bronze when they exist in the source. Silver and gold models exclude them.

`gold.dim_patient` is sourced only from `data/patients.csv` through `silver.patients`. Synthetic patient records from `healthcare_dataset.csv` are not added to the patient dimension.

All gold dimension and fact assets include blocking row-count custom checks. A `bruin run` fails if any target warehouse table materializes with zero rows.

## Configure

Set Bruin connections in `.bruin.yml` or override them in your local Bruin environment:

- `healthcare_gcp`: Google Cloud Platform connection used by BigQuery SQL and seed assets
- `healthcare_gcs`: GCS source connection used by the streaming vitals ingestion asset

The default project is `de-zoomcamp-493207`; update `pipeline.yml` if your BigQuery project differs.

## Run

Validate all assets:

```bash
bruin validate pipeline
```

Before the first warehouse run, use fast validation because BigQuery dry-run validation expects upstream bronze and silver tables to already exist:

```bash
bruin validate pipeline --fast
```

Run the full pipeline:

```bash
bruin run pipeline
```

Inspect lineage:

```bash
bruin lineage pipeline/assets/gold/fct_vitals_sign.sql
```
