# Data Engineering — Technical Assignment

## How to Run

### 1. Create the Python environment

```bash
python -m venv .venv
source .venv/bin/activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Start the data API

```bash
docker compose up --build
```

Verify it's running:
```bash
curl http://localhost:5050/health
```

### 4. Run the pipeline

```bash
python -m src.helu.pipelines.mrr_pipeline
```

The pipeline will run all stages in order — landing → bronze → silver → gold — and write the final MRR report to `output/gold/mrr_financial_report/`.

---

## Project Structure

```
data-challenge/
├── app.py                          # Docker API (do not modify)
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── data/                           # Raw CSV source files (fallback if Docker unavailable)
│   ├── apfel_subscriptions.csv
│   ├── fenster_subscriptions.csv
│   └── exchange_rates.csv
├── output/                         # Generated pipeline output (Delta tables)
│   ├── landing/
│   ├── bronze/
│   ├── silver/
│   └── gold/
└── src/
    └── helu/
        ├── pipelines/
        │   └── mrr_pipeline.py     # Entry point — runs the full pipeline
        ├── landing/                # Ingestion from API → local files
        ├── bronze/                 # Raw ingestion → Delta tables
        ├── silver/                 # Cleaning, validation, normalisation
        ├── gold/                   # Final MRR report aggregation
        ├── utils/                  # Shared: JobParameters, Writer, configs
        └── tests/                  # Unit tests per layer
            ├── test_landing/
            ├── test_bronze/
            ├── test_silver/
            └── test_gold/
```

---

## How to Query the Report

A notebook `query_tables.ipynb` is provided at the root of the project. For simplicity, querying was done this way — 
each cell reads the Delta tables and registers them as temporary views, so custom queries can be written directly in
subsequent cells using SparkSQL.

---

## Design Decisions

### Architecture — Medallion Layers

For this project the medallion architecture was chosen to structure the pipeline.
It is divided into 4 layers: landing, bronze, silver and gold.
Each layer has a specific purpose and transformations are applied
as the data moves through the layers.

- **Landing**: this is the raw ingestion layer where data is fetched from the API and stored as-is in local files.
The landing layer folder is divided into three subfolders, and inside each subfolder the data is categorised by source and organised by date.

```
output/
├── landing/  
    ├── inbound/
    ├── processed/
    └── error/
```

The subfolders are:
Inbound: where arriving data is stored when it is fetched from the API, organised as follows:
`/landing/inbound/{source}/yyyy-mm-dd/data`
Once the data is processed in subsequent steps it is moved to either the processed subfolder if the processing was successful, or to the error subfolder if there were any issues during processing.

- **Bronze**: This layer is responsible for storing the raw data in a structured format (Delta Lake tables) without any transformations, or minor transformations in the Apfel case.
The data is append-only and partitioned by `_inserted_date_utc`, which is the date on which the data was moved to the bronze layer. The data is stored by source:
```
output/
├── bronze/  
    ├── apfel/
    ├── exchange_rates/
    └── fenster/
```
In here information such as dates and timestamps are stored as strings, and the data is not cleaned or validated yet.

- **Silver**: This layer is responsible for cleaning, validating and normalising the data. In this layer the data is transformed to have the correct data types,
missing values are handled and normalisations in the data are resolved. The silver tables are partitioned by `inserted_date_utc`, and the load from the bronze layer is done
incrementally in a customisable way where it is possible to specify which column to use as a watermark to only load new data from the bronze layer.
The silver delta tables are stored by source:
```
output/
├── silver/  
    ├── apfel/
    ├── exchange_rates/
    └── fenster/
```

- **Gold**: This layer is responsible for the final aggregation and business logic to generate the MRR report.
In this layer the data from the silver tables is joined and aggregated to calculate the MRR per month, 
and the final report is stored as a Delta Lake table partitioned by `report_month` for faster querying.
```
output/
├── gold/  
    └── mrr_financial_report/
    
```

The pipeline for the project can be found in `src/helu/pipelines/mrr_pipeline.py` and it runs all the stages in order — landing → bronze → silver → gold.

The project was structured in a modular way, where each layer and its dependencies are well defined and separated. This allows that
as the number of pipelines or entities grows, the organisation and maintenance of the codebase is easier, and also allows code reuse across pipelines if needed.

the codebase is structured as follows:

```
src/
└── helu/
    ├── pipelines/
    │   └── mrr_pipeline.py     # Entry point — runs the full pipeline
    ├── landing/                # Ingestion from API → local files
    ├── bronze/                 # Raw ingestion → Delta tables
    ├── silver/                 # Cleaning, validation, normalisation
    ├── gold/                   # Final MRR report aggregation
    ├── utils/                  # Shared: JobParameters, Writer, configs
    └── tests/                  # Unit tests per layer
 ```
#### The `utils` folder
Can be considered as the core of the project, it contains the shared logic across the layers as well as the individual core logic for each layer.
It is divided into:
Scripts that are used transversally across the layers such as:
- `job_parameters.py`: contains the logic to handle the parameters of the jobs.
- `writer.py`: contains the logic to write the data to the output folders in the different layers, it handles the writing of the data in a consistent way across the layers and also handles the partitioning of the data.
- `configs.py`: contains the configuration of the different layers such as the schema of the tables, the paths of the output folders, and other configurations that are used across the layers.
- `tranversal_methods.py`: contains methods that are used across the layers.
Folders with the core logic of each layer:
`bronze_core`: contains the logic for the jobs in charge of ingesting the data into the bronze layer for the different sources.
    It contains and main class that serves as templates for more custom ingestion but can be used as it is for simple ingestion jobs as well to avoid code duplication.
`silver_core`: contains the logic for the jobs in charge of cleaning, validating and normalising the data for the different sources in the silver layer. 
It contains and main class that serves as templates for more custom silver jobs but can be used as it is for simple cleaning and normalisation jobs as well to avoid code duplication.
`gold_core`: contains the logic for the jobs in charge of the final aggregation and business logic to generate the MRR report in the gold layer. As it is the only job 
it doesn't have a main class template but it allows that in the future if needed it can be created

#### The `bronze`, `silver`, `gold` folder
These folders act as the main entrypoint of each job, they contain the code to run the job for each source and layer.
This was designed this way for easier maintenance, readability and use in pipelines, as well as to have a clear separation between the code that is in charge of running the job and the code that contains the core logic of the job, which is in the `utils` folder.

### Quality Assurance
The project contains a set of unit tests for each layer, which can be found in the `src/helu/tests` folder.
Each layer has its own subfolder with the tests for that layer. 
The tests are designed to test the core logic of each layer in isolation, using in-memory DataFrames to avoid the need for a running cluster or access to the API. 
The tests cover various scenarios to test and keep the robustness of the pipeline.

```
tests/
├── test_bronze/
├── test_gold/
├── test_landing/
├── test_silver/
└── test_utils/

 ```

### Transformations, considerations and assumptions
The audit columns added in each layer such as `_inserted_date_utc` can be identified by the use of the underscore at the beginning and end of the column name, 
this was design this way to make it clear that these columns are not part of the original data and are added for auditing purposes, and also to avoid any potential conflicts with existing columns in the data.
#### Landing to Bronze
- No transformations were done except for exploding the Apfel JSON instead of leaving it as-is.
- The timestamps were inserted as strings.
- Optionally we check schema validation for the incoming data, if the data doesn't match the expected schema it is moved to the error folder in the landing layer,
and if it matches it is moved to the processed folder in the landing layer, 
this was designed this way to have a clear separation of the data that is valid and the data that is not valid, 
and also to have a clear way to identify and handle errors in the ingestion process. However, for exchange rates this was not added so it can have
schema evolution without requiring any code changes.
#### Bronze to Silver
- Name normalisation of columns to create informative and consistent column names across sources, for example `customer_id` instead of `c_id` or `uuid`.
- Columns that were giving the same information but with different names across sources were unified, for example `price` and `amount` were unified to `price_amount`.
- Checks of valid ENUM values for columns such as currency and country codes.
- Deduplication of records in both Apfel and Fenster tables based on `customer_id` and taking the last record using the `event_timestamp` column
- For exchange rates as it is a small dimension a full load is done every time.
- The data types of the columns were changed to the correct ones, for example `price_amount` was changed to `Decimal` and `event_timestamp` was changed to `Timestamp`.
- When data is not valid such as null values or invalid values the data is moved to another folder for audit and to not break the process.

#### Silver to Gold
- A QA function was added to check the data before writing into the gold layer, in this schema mismatches, 
null values or empty DataFrames are checked and if any of these checks fail the data is not written to the 
gold layer and an error is raised.

### Implementation

**PySpark** was chosen as the processing engine because the pipeline joins and aggregates data across multiple sources 
(two subscription platforms and exchange rates), which maps naturally to DataFrame
transformations and SQL. It also makes the logic easy to test in isolation using in-memory DataFrames without needing a 
running cluster.

The output is stored as **Parquet files with a Delta Lake layer** on top. Parquet provides an efficient columnar 
format that is directly queryable (e.g. via DuckDB or Spark). 
Delta Lake adds ACID transaction guarantees, which means the pipeline can be re-run safely — 
each write is atomic and idempotent — without corrupting existing data or producing duplicate records.

---

## Next Steps

- **Monitoring & alerting**: expose QA check results (null counts, schema mismatches, empty reports) to an observability tool so failures are surfaced without needing to inspect logs manually.
- **Expand test coverage**: add integration tests that run the full pipeline end-to-end.
- **Implement test layers**: currently there are only unit tests for the gold layer to not delay the project, but
unit tests for each method of the bronze, silver and utils scripts should be added to have broader coverage.
- **Handling of timezones and timestamps**: currently the pipelines assume the data for time values is not malformed, but
a validation and handling should be added to avoid issues, especially since there are different timezones or possible changes in
the format of the timestamps in the future.
- **Ingestion in landing**: currently the JSON is saved as it comes from the API, but as data grows it may become
too large to manage as a single file, so splitting the JSON into smaller files or finding a way to read the API
in smaller chunks could be implemented.
- **Parallelisation**: currently the pipeline runs sequentially, but the ingestion of Apfel, Fenster and Exchange rates
can be done in parallel, respecting the dependencies for gold to run downstream.
