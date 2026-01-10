# Big Data Product demo Architecture with Databricks Asset Bundles and organizational toolkit

## Project Overview
This is a **feedback data product** for Organization A. The project implements modern PySpark-based ETL pipelines with a four-layer architecture (Landing → Raw → Curated → Curated History) for processing customer feedback, satisfaction surveys, and text analytics in Databricks.

## Data Pipeline Architecture

The pipeline orchestrates data flow through five automated stages, managed by Databricks Asset Bundles and orgdp-toolkit accelerators:

### **Stage 1: Preflight Checks**
Environment and configuration validation before pipeline execution begins.

### **Stage 2: Source → Landing (Ingestion Layer)**
- **Sources**: SQL Server databases (feedback_db, analytics_db), AWS S3, Excel/CSV files
- **Process**: Execute SQL queries with incremental loads (typically last 1 week of data), fetch 20+ feedback tables and mapping data
- **Output**: Parquet files written to ADLS landing zone (`abfss://...landing/{table_name}/delta/*.parquet`)
- **Entry Point**: `ingest_from_sources_to_landing` → orchestrates all `ingest_*.py` modules
- **Security**: All credentials retrieved from Databricks secret scopes (`sql-db-secrets`, `s3-secrets`, `api-secrets`) with environment-based naming

### **Stage 3: Landing → Raw (Raw Data Layer)**
- **Process**: Autoloader incrementally reads landing files, enforces schema, captures malformed data in rescued column
- **Transformations**: Text sanitization (NLP API for sensitive content removal) + PII anonymization (name/address registries, regex patterns for email, phone, SSN, IBAN)
- **Output**: Delta tables in Unity Catalog raw layer (e.g., `{catalog}.{schema}.feedback_table_1`, `{catalog}.{schema}.feedback_table_2`)
- **Entry Point**: `landing_to_raw_managed_stage` (orgdp-toolkit accelerator) with `raw_preprocess` transformation
- **Features**: Schema evolution disabled, rescued data column (`_rescued_data`), append-only writes, technical metadata fields

### **Stage 4: Raw → Curated (Curated Data Layer)**
- **Transformation Sub-stage** (`main_transformations_for_raw`):
  - ML-based sentiment analysis and topic clustering via NLP API
  - Text categorization (main_category, sub_category, topic)
  - Satisfaction metrics calculation and aggregation for group reporting
  - Output: Intermediate transformed tables (`sanitized_feedback_table_{table}`, `aggregated_satisfaction_metrics`)
- **Merge Sub-stage** (`raw_to_source_table_managed_stage`):
  - Deduplication using primary keys and hash-based change detection
  - Great Expectations data quality validation (schema compliance, freshness within 100 days, value sets, row counts)
  - MERGE operations into curated Delta tables with upsert logic
  - Data quality publishing to Databricks
- **Output**: Production-ready curated tables optimized for analytics and reporting

### **Stage 5: Curated → Curated History (Historical Data Layer)**
- **Process**: Maintain historical snapshots with SCD Type 2 (Slowly Changing Dimension) using hash-based change tracking
- **Output**: Historical curated tables with `_history` suffix, including validity timestamps and hash columns
- **Entry Point**: `raw_to_source_history_table_managed_stage` (orgdp-toolkit accelerator)

**Orchestration**: All stages run sequentially via Databricks workflow jobs defined in `resources/source-aligned-job.yml`, with parallel task execution where dependencies allow. Job failures trigger email and webhook notifications configured per environment.

For Databricks Asset Bundle (DAB) deployment and configuration instructions, see [DAB_INSTRUCTIONS.md](DAB_INSTRUCTIONS.md).

## Core Technologies
- **Primary Framework**: PySpark (Apache Spark with Python)
- **Data Platform**: Databricks Unity Catalog / Delta Lake
- **Orchestration**: orgdp-toolkit accelerators for managed data stages
- **API Integration**:
  - NLP API for sentiment analysis and text clustering/categorization
  - NLP API for text sanitization (sensitive content removal)
  - SQL Server (JDBC) for source data ingestion
- **Data Storage**: Delta Lake tables in Unity Catalog with ACID transactions
- **Data Formats**: Parquet (landing), Delta (raw/curated), CSV/Excel (file sources)
- **Text Processing**:
  - PII anonymization using name/address registries and regex patterns
  - Text sanitization via external NLP API
  - ML-based sentiment analysis and topic clustering
- **Data Quality**: Great Expectations for schema validation, freshness checks, and completeness validation
- **Security**: Databricks secret scopes for credential management with environment-based naming
- **Testing**: Unit tests with PySpark DataFrames and comparison against reference outputs

## Project Structure
```
src/demodataproduct/
├── entry_points/
│   └── catapult.py                    # Pipeline orchestration entry points
├── config/
│   ├── pipeline_config.yml            # Non-sensitive configuration (servers, databases, paths)
│   ├── config_loader.py               # Configuration loading utilities
│   └── application/                   # Table metadata and application configuration
│       ├── application_metadata.py    # Data application metadata definitions
│       └── target_tables/             # Table schemas, validators, and transformations
│           ├── common_transformations.py          # Reusable transformation functions
│           ├── common_data_quality_expectation.py # Shared validation decorators
│           └── *.py                               # Individual table metadata files
├── cx_feedback/
│   ├── ingestions/                    # Data ingestion from sources to landing
│   │   ├── ingest_*.py                # Ingestion pipelines (SQL, S3, files)
│   │   └── query_*.py                 # SQL query definitions
│   ├── transformations/               # Data transformation logic
│   │   ├── anonymization/             # PII anonymization (names, addresses, emails, etc.)
│   │   ├── artificial_intelligence/   # Text sanitization via NLP API
│   │   ├── response_clustering/       # ML-based sentiment and topic clustering
│   │   ├── nps_reporting/             # NPS calculation and aggregation
│   │   └── macro/                     # Reusable transformation components
│   ├── writes/                        # Data write operations (e.g., to SQL databases)
│   └── help/                          # Deprecated - utilities migrated to utils/
└── utils/
    ├── etl_utilities.py               # ETL helper functions (get_secret, read_file_source, etc.)
    ├── helpers.py                     # General helper functions
    └── datahub_helper.py              # DataHub integration helpers
```

## Architectural Diagram
```
┌────────────────────────────────────────────────────────────────────┐
│ SOURCE SYSTEMS                                                     │
│ • SQL Server (feedback_db, analytics_db)                          │
│ • AWS S3 (feedback JSON files)                                    │
│ • ADLS Files (Excel mapping files, CSV data)                      │
└────────────┬───────────────────────────────────────────────────────┘
             │
             │ ingest_*.py (SQL queries, S3 reads, file reads)
             │ Credentials from Databricks secret scopes
             ▼
┌────────────────────────────────────────────────────────────────────┐
│ LANDING ZONE                                                       │
│ abfss://.../landing/{table_name}/delta/*.parquet                  │
│ • 20+ feedback tables (feedback_table_1 through feedback_table_21) │
│ • Satisfaction survey data                                        │
│ • Mapping tables (reference_table, map_table_1-3)                 │
└────────────┬───────────────────────────────────────────────────────┘
             │
             │ landing_to_raw_managed_stage()
             │ (Autoloader + raw_preprocess transformation)
             │ • Text sanitization (NLP API)
             │ • PII anonymization (registries + regex)
             ▼
┌────────────────────────────────────────────────────────────────────┐
│ RAW LAYER (Delta Tables)                                          │
│ {catalog}.{schema}.{table_name}                                   │
│ • Schema enforcement with _rescued_data column                    │
│ • Sanitized & anonymized text                                     │
│ • Technical metadata fields (load_timestamp, etc.)                │
└────────────┬───────────────────────────────────────────────────────┘
             │
             │ main_transformations_for_raw()
             │ • ML sentiment analysis & topic clustering (NLP API)
             │ • Satisfaction calculation & aggregation
             ▼
┌────────────────────────────────────────────────────────────────────┐
│ TRANSFORMED TABLES (Intermediate)                                  │
│ • sanitized_feedback_table_{table}                                │
│ • aggregated_satisfaction_metrics                                 │
└────────────┬───────────────────────────────────────────────────────┘
             │
             │ raw_to_source_table_managed_stage()
             │ • Deduplication (primary keys + hash)
             │ • Great Expectations validation
             │ • MERGE operations (upsert)
             ▼
┌────────────────────────────────────────────────────────────────────┐
│ CURATED LAYER (Delta Tables)                                      │
│ Production-ready tables for analytics & reporting                 │
│ • sanitized_feedback_table_1                                      │
│ • sanitized_feedback_table_2                                      │
│ • aggregated_satisfaction_metrics                                 │
│ • ... (7 total sanitized feedback tables)                         │
└────────────┬───────────────────────────────────────────────────────┘
             │
             │ raw_to_source_history_table_managed_stage()
             │ SCD Type 2 with hash-based change tracking
             ▼
┌────────────────────────────────────────────────────────────────────┐
│ CURATED HISTORY LAYER (Delta Tables)                              │
│ Historical snapshots with validity timestamps                     │
│ • {table_name}_history                                            │
└────────────────────────────────────────────────────────────────────┘
```

## Key Pipeline Components

### 1. Ingestion Layer (Source → Landing)
- **Purpose**: Extract data from source systems and write to landing zone in Parquet format
- **Main Files**:
  - `ingest_feedback_table_1.py` - Feedback table 1 from feedback_db database
  - `ingest_feedback_table_2.py` - Feedback table 2 from feedback_db database
  - `ingest_analytics_table_1.py` - Analytics data from analytics_db database
  - `ingest_feedback_source_s3.py` - Feedback data from AWS S3 JSON files
  - `ingest_file_sources.py` - Excel mapping files and CSV data from ADLS
- **Key Features**:
  - Secure credential management via Databricks secret scopes with environment-based naming
  - SQL Server connectivity with parameterized queries and JDBC
  - Multi-format file reading (Excel with multiple sheets, CSV, JSON, Delta)
  - Incremental loading (typically last 7 days) with configurable date filters
  - Error handling with graceful degradation for missing sources
  - Parquet output optimized for Spark autoloader

### 2. Raw Layer (Landing → Raw)
- **Purpose**: Load landing files into Delta tables with schema enforcement and initial data cleaning
- **Transformation**: `raw_preprocess` function defined in table metadata
  - Text sanitization via NLP API (removes sensitive content)
  - PII anonymization using name/address registries and regex patterns
  - Removes: names, addresses, emails, phone numbers, SSNs, IBANs, account numbers, vehicle registrations
- **Main Files**:
  - `transformations/artificial_intelligence/response_sanitization.py` - NLP API integration for sanitization
  - `transformations/anonymization/response_anonymization.py` - Comprehensive PII removal
  - `config/application/target_tables/common_transformations.py` - Reusable preprocessing functions
- **Key Features**:
  - Autoloader for incremental file processing
  - Schema evolution disabled for data quality
  - Rescued data column (`_rescued_data`) for malformed records
  - Technical metadata fields (load_timestamp, _source_file, etc.)
  - Append-only writes with merge schema option

### 3. Transformation Layer (Raw → Transformed Tables)
- **Purpose**: Apply ML-based analytics and business logic to create analysis-ready datasets
- **Main Files**:
  - `transformations/response_clustering/feedback_analytics_clustering.py` - Sentiment and topic clustering
  - `transformations/macro/clustering_macro.py` - NLP API wrapper for text clustering
  - `transformations/macro/cleanse.py` - Text normalization and cleaning
  - `transformations/satisfaction_reporting/satisfaction_aggregation.py` - Satisfaction metrics calculation and aggregation
- **Key Features**:
  - ML-based sentiment analysis (positive, negative, neutral)
  - Multi-level topic clustering (main_category, sub_category, topic)
  - NPS calculation with promoter/detractor segmentation
  - Batch processing for API efficiency
  - Comprehensive error handling with fallback to original data

### 4. Curated Layer (Transformed → Curated)
- **Purpose**: Deduplicate, validate, and merge data into production-ready tables
- **Managed by**: orgdp-toolkit `raw_to_source_load` accelerator
- **Key Features**:
  - Hash-based deduplication using primary keys (key_hash for unique, attr_hash for attributes)
  - Great Expectations data quality validation:
    - Schema compliance (required columns, data types)
    - Freshness validation (response_date within 100 days)
    - Value set constraints (sentiment in predefined values)
    - Row count thresholds (0-1000 rows)
  - MERGE operations with upsert logic based on hash changes
  - Data quality reporting to Databricks
  - Delta table optimization and statistics

### 5. Curated History Layer (Curated → History)
- **Purpose**: Maintain historical snapshots with SCD Type 2 for change tracking
- **Managed by**: orgdp-toolkit `raw_to_source_history_load` accelerator
- **Key Features**:
  - Hash-based change detection (key_hash + attr_hash)
  - Validity timestamps (valid_from, valid_to)
  - Full historical lineage for audit and time-travel queries
  - Optimized Delta storage with partitioning

### 6. Data Quality Framework
- **Tool**: Great Expectations integrated via orgdp-toolkit
- **Implementation**: Decorators and validators in `config/application/target_tables/`
- **Common Validators**:
  - `@common_validation` - Standard schema and null checks
  - `@relaxed_response_clustering_common_validation` - Flexible validation for ML-transformed tables
- **Custom Validators**: Table-specific functions for business rule validation
- **Validation Modes**:
  - `delta` - Only validate changed/new records (efficient for incremental loads)
  - `full` - Validate entire table (comprehensive but slower)

## Landing Layer Table Schemas

**Tables of open end customer feedback:**
- `feedback_table_1` - Feedback table 1
- `feedback_table_2` - Feedback table 2
- `feedback_table_3` - Feedback table 3
- `feedback_table_4` - Feedback table 4
- `feedback_table_5` - Feedback table 5
- `feedback_table_6` - Feedback table 6
- `feedback_table_7` - Feedback table 7
- `feedback_table_8` - Feedback table 8
- `feedback_table_9` - Feedback table 9
- `feedback_table_10` - Feedback table 10
- `feedback_table_11` - Feedback table 11
- `feedback_table_12` - Feedback table 12
- `feedback_table_13` - Feedback table 13
- `feedback_table_14` - Feedback table 14
- `feedback_table_15` - Feedback table 15
- `feedback_table_16` - Feedback table 16
- `feedback_table_17` - Feedback table 17
- `feedback_table_18` - Feedback table 18
- `feedback_table_19` - Feedback table 19
- `feedback_table_20` - Feedback table 20
- `feedback_table_21` - Feedback table 21

**Other tables:**
- `feedback_table_22` - Feedback table 22
- `feedback_table_23` - Feedback table 23
- `reference_table` - Reference table for channels
- `map_table_1` - Mapping table 1
- `map_table_2` - Mapping table 2
- `map_table_3` - Mapping table 3
- `aggregated_satisfaction_metrics` - Aggregated satisfaction metrics

**Common Schema for Open-End Feedback Landing Tables:**
(All tables from `feedback_table_1` through `feedback_table_21` share this schema after sanitization and anonymization)
- `ID` - Unique identifier for the response (composite key format varies by source)
- `RESPONSE_DATE` - Response date/timestamp (non-null)
- `RESPONSE_TEXT` - Response text (open-end feedback, sanitized and anonymized)
- `QUESTION_TEXT` - Question text (only in survey tables)
- `SURVEY_ID` - Survey identifier (only in survey tables)
- `load_timestamp` - Data load timestamp
- `ingestion_timestamp` - Data ingestion timestamp (added during Delta append)

**Special Notes:**
- `feedback_table_3` may use different source column names than the standard schema
- `feedback_table_7` may include additional question and survey identifier columns
- All feedback text in `RESPONSE_TEXT` column is processed through sanitization (sensitive content removal) and anonymization (PII removal) pipelines
- Data is incrementally loaded (typically last 1 week)

**Common Schema for "map_*" Tables:**
- `Rivijärjestys` - The order for the rows in report int
- `Kanava_excel` - The name for each row in the final report
- `Lähde` - The name to match when joining tables

**Schema for reference_table:**
- `organization_name` - Organization string
- `organization_order` - Organization order int
- `reporting_category` - Reporting channels string
- `reporting_order` - Reporting channel order int
- `channel_name` - Channel string
- `channel_order` - Channel order int
- `channel_abbreviation` - Channel abbreviation if exist string
- `channel_report_name` - Channel name in reporting summary string
- `channel_source_identifier` - Channel source name for mapping with "map_*" tables.

**Schema for feedback_table_1:**
- `id` - Customer identifier (non-null)
- `response_date` - Date of response (non-null, freshness validated within 100 days)
- `satisfaction_score` - Satisfaction score value
- `language` - Language code
- `path_identifier` - Process path identifier
- `response_text` - Anonymized feedback text
- `file_date` - File creation date
- `service_type` - Service type
- `channel_type` - Channel type

**Schema for feedback_table_22:**
- `Date` - Date string
- `region` - Region / location
- `session_id` - Session identifier string
- `agent_primary` - Primary agent name
- `room_id` - Room identifier int
- `agent_primary_id` - Primary agent id int
- `agent_secondary` - Secondary agent name
- `agent_secondary_id` - Secondary agent id int
- `satisfaction_score` - Satisfaction score value
- `feedback_text` - Open end feedback

**Schema for feedback_table_23:**
- `Date` - Date string
- `queue_response_time` - Queue response time identifier
- `queue_received` - Queue received identifier
- `call_answered` - Call answered identifier
- `call_ended` - Call ended identifier
- `call_direction` - Call direction code
- `satisfaction_question` - Satisfaction survey answer (0-10) int


## Refined/Transformed Layer Table Schemas

### Response Clustering Tables (Curated Layer)

The response clustering pipeline processes customer feedback from multiple sources. These tables contain ML-enriched data with sentiment analysis and topic clustering.

**Common Schema for Response Clustering Tables:**
- `user_id` - Customer identifier (non-null, part of primary key)
- `question_id` - Survey question identifier (non-null, part of primary key)
- `response_id` - Unique response identifier (non-null, part of primary key)
- `response_date` - Date of response (non-null, freshness validated within 100 days)
- `sentiment` - Response sentiment classification (values: "positive", "negative", "neutral", "" for empty)
- `main_category` - Primary category from ML clustering
- `sub_category` - Subcategory from ML clustering
- `topic` - Detailed topic from ML clustering
- `key_hash` - Unique hash for deduplication (calculated from primary keys)
- `attr_hash` - Attribute hash for change detection (calculated from non-key columns)
- `ingestion_timestamp` - Timestamp when key_hash first appeared (immutable)
- `update_timestamp` - Timestamp when attr_hash changed (updates on attribute changes)

**Curated Tables:**
- `sanitized_feedback_table_1` - Feedback table 1 with sentiment and clustering
- `sanitized_feedback_table_2` - Feedback table 2 with sentiment and clustering
- `sanitized_feedback_table_3` - Feedback table 3 with sentiment and clustering
- `sanitized_feedback_table_4` - Feedback table 4 with sentiment and clustering
- `sanitized_feedback_table_5` - Feedback table 5 with sentiment and clustering
- `sanitized_feedback_table_6` - Feedback table 6 with sentiment and clustering
- `sanitized_feedback_table_7` - Feedback table 7 with sentiment and clustering

**Data Quality Validations:**
- Non-null enforcement on primary key columns (user_id, question_id, response_id or id)
- Sentiment values restricted to predefined set: ["positive", "negative", "neutral", ""]
- Response date freshness within 100 days from current date
- Row count validation (0-1000 rows for delta mode validation)
- Schema compliance (all expected columns present with correct data types)

### Group NPS Report Table (Curated Layer)

**Schema for `aggregated_satisfaction_metrics`:**
- `year_month` - Reporting period in YYYY-MM format (non-null, primary key)
- `satisfaction_index` - Satisfaction Index value (calculated metric)
- `response_count` - Total number of survey responses
- `promoter` - Count of promoter responses (score 9-10)
- `detractor` - Count of detractor responses (score 0-6)
- `organization_name` - Organization name
- `organization_order` - Organization display order (int)
- `reporting_category` - Reporting category/grouping
- `reporting_order` - Reporting category display order (int)
- `channel_name` - Communication channel
- `channel_order` - Channel display order (int)
- `channel_short` - Abbreviated channel name
- `channel_table` - Channel reference for table joins
- `source_name` - Data source identifier
- `key_hash` - Unique hash (year_month + channel + organization)
- `attr_hash` - Attribute hash for change detection
- `ingestion_timestamp` - First load timestamp
- `update_timestamp` - Last update timestamp

**Data Quality Validations:**
- Non-null enforcement on `year_month`
- Row count validation (0-1000 rows)

## Coding Standards & Patterns

### PySpark Best Practices
```python
# Preferred DataFrame transformation pattern
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, lit, regexp_replace, trim

def transform_data(df: DataFrame) -> DataFrame:
    """Apply transformations with proper error handling."""
    try:
        result = df.withColumn("cleaned_text",
                    regexp_replace(trim(col("text")), r"[^\w\s]", ""))
        return result
    except Exception as e:
        log.error(f"Transformation failed: {e}")
        return df
```

### Date Parsing Patterns
```python
# Multi-format date parsing approach
def parse_multiple_date_formats(df: DataFrame, date_col: str, use_advanced_parsing: bool = False) -> DataFrame:
    if use_advanced_parsing:
        return normalize_and_parse_date(df, col_name=date_col, two_digit_cutoff=68, swap_ambiguous_dmy=True)

    return df.withColumn(date_col,
        when(to_date(col(date_col), "yyyy-MM-dd").isNotNull(),
             to_date(col(date_col), "yyyy-MM-dd"))
        .when(to_date(col(date_col), "M/d/yy").isNotNull(),
              to_date(col(date_col), "M/d/yy"))
        .otherwise(col(date_col)))
```

### Error Handling Pattern
```python
# Standard error handling with logging
log = logging.getLogger(__name__)

try:
    # Processing logic
    result_df = process_data(input_df)
    log.info(f"Processing completed: {result_df.count()} rows")
    return result_df
except Exception as e:
    log.error(f"Processing failed: {e}", exc_info=True)
    # Return empty DataFrame with expected schema on failure
    return spark.createDataFrame([], expected_schema)
```

## Function Naming Conventions
- **Ingestion Functions**: `ingest_<source_name>()` or script-level execution
- **Query Functions**: `<source>_sql_source_queries()` returns dict of SQL queries
- **Processing Functions**: `process_<description>()` or `<action>_<noun>()`
- **Utility Functions**: `<verb>_<noun>()` (e.g., `parse_date`, `validate_schema`)
- **Anonymization Functions**: `<type>_anonymization()` (e.g., `name_register_anonymization`)
- **Helper Functions**: Descriptive names in `etl_utilities.py` (e.g., `read_file_source`, `append_dataframes_to_delta`)

## Data Validation Patterns
```python
# Schema validation
def validate_required_columns(df: DataFrame, required_cols: List[str]) -> bool:
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        log.warning(f"Missing columns: {missing_cols}")
        return False
    return True

# Data quality checks
def validate_data_quality(df: DataFrame) -> Dict[str, Any]:
    return {
        "total_rows": df.count(),
        "null_responses": df.filter(col("response").isNull()).count(),
        "empty_responses": df.filter(col("response") == "").count()
    }
```

## API Integration Pattern
```python
# NLP API integration approach
def classify_responses(df_batch: DataFrame, api_key: str, api_url: str) -> DataFrame:
    """Batch process responses through NLP API."""
    # Convert to pandas for API calls (necessary for current API client)
    pandas_batch = df_batch.toPandas()

    # Process through API
    result_pandas = call_nlp_api(pandas_batch, api_key, api_url)

    # Convert back to Spark DataFrame
    return spark.createDataFrame(result_pandas)
```

## Configuration Management

### Credential Management (Databricks Secret Scopes)
All sensitive credentials are stored in Databricks secret scopes with environment-based naming:

**SQL Database Credentials** (`sql-db-secrets` scope):
- `SQL_USER_{env}` - Database username
- `SQL_PASSWORD_{env}` - Database password

**API Credentials** (`api-secrets` scope):
- `SANITIZATION_API_KEY_{env}` - NLP sanitization API key
- `SANITIZATION_API_URL_{env}` - NLP sanitization API endpoint
- `CLUSTERING_API_KEY_{env}` - NLP clustering API key
- `CLUSTERING_API_URL_{env}` - NLP clustering API endpoint

**AWS S3 Credentials** (`s3-secrets` scope):
- `S3_ACCESS_KEY_{env}` - AWS access key for S3 bucket access
- `S3_SECRET_KEY_{env}` - AWS secret key for S3 bucket access

**Usage Pattern**:
```python
from demodataproduct.utils.etl_utilities import get_secret

# Automatically appends environment suffix (dev/test/qa/prod)
api_key = get_secret("SANITIZATION_API_KEY", scope="api-secrets")
db_user = get_secret("SQL_USER", scope="sql-db-secrets")

# Falls back to os.getenv() for local development
```

### Non-Sensitive Configuration (pipeline_config.yml)
Server names, database names, and other non-sensitive settings:

```yaml
sql_databases:
  feedback_db:
    server: "feedback-server.example.com"
    database: "feedback_db"
    port: 1433

  analytics_db:
    server: "default"  # Configure per environment
    database: "analytics_db"
    port: 1433

s3:
  endpoint_url: "https://s3-eu-central-1.amazonaws.com"
  bucket: "demo-feedback-bucket"

api:
  sanitization:
    batch_size: 100
    timeout_sanitize: 60

  clustering:
    timeout_combined: 60
```

**Usage Pattern**:
```python
from demodataproduct.config.config_loader import get_config

pipeline_config = get_config()
db_config = pipeline_config.get_sql_database_config('feedback_db')
server = db_config['server']  # "feedback-server.example.com"
database = db_config['database']  # "feedback_db"
```

### Environment Variable
- `ENVIRONMENT` - Determines environment suffix for secrets (dev/test/qa/prod)
- Set automatically in Databricks jobs, or manually for local development

### Unity Catalog Configuration
- **Catalog**: Environment-specific (e.g., `shared_landing_dev`, `shared_landing_prod`)
- **Schema**: Application namespace (e.g., `demodataproduct`)
- **Tables**: Named according to table metadata definitions

For complete secret setup instructions, see [SECRETS_SETUP.md](SECRETS_SETUP.md).

## Testing Approach
- **Unit Tests**: Individual function validation
- **Integration Tests**: End-to-end pipeline testing
- **Comparison Tests**: Validate against Alteryx reference outputs
- **Data Quality Tests**: Schema and content validation

## Performance Considerations
- Use `.coalesce(1)` for small output files
- Implement proper partitioning for large datasets
- Cache intermediate results when reused
- Use broadcast joins for small lookup tables

## Error Recovery Patterns
- Implement graceful degradation for missing data sources
- Use empty DataFrames with proper schema as fallbacks
- Log all errors with context for debugging
- Continue processing other datasets when one fails

## Documentation Standards
- Include comprehensive docstrings with Args/Returns
- Document expected input/output schemas
- Provide transformation logic explanations
- Include performance and memory considerations

## Delta Lake Patterns
```python
# Append to Delta table
from demodataproduct.cx_feedback.help.etl_utilities import append_dataframes_to_delta

dataframes = {"table_name": df}
append_dataframes_to_delta(
    dataframes=dataframes,
    catalog="landing_shared_dev",
    schema="demodataproduct",
    spark=spark
)
```

## Anonymization Patterns
```python
# Using name/address registers for PII removal
from pyspark.sql.functions import col

# Create registers
address_register = create_address_register(spark, dat_file_path="path/to/BAF.dat")
name_register = create_name_register(spark, name_path_list=["path/to/names/"])

# Anonymize text column
anonymized_df = comprehensive_anonymize_text_without_api_spark(
    spark=spark,
    texts=df,
    text_col="response",
    tag=True,  # Use <tags> instead of replacement values
    address_register_df=address_register,
    name_register_dict=name_register
)
```

## SQL Query Definition Pattern
```python
# Define queries in query_*.py files
def source_sql_queries():
    """Return dictionary of SQL queries with descriptive keys."""
    queries = {
        "table_name_descriptive": """
            SELECT
                column1,
                column2
            FROM schema.table
            WHERE date_column >= DATEADD(DAY, -1, GETDATE())
        """,
    }
    return queries
```

## Best Practices

### Code Quality
1. **Always use PySpark DataFrame operations** over pandas when possible for scalability
2. **Include proper error handling** with logging using Python's logging module
3. **Follow established naming conventions** for functions, variables, and files
4. **Use type hints** for function parameters and return values
5. **Include comprehensive docstrings** with Args, Returns, and usage examples
6. **Validate data quality** with schema checks, null handling, and Great Expectations

### Data Management
7. **Use Delta Lake for all persistent storage** - never write Parquet directly for final tables
8. **Respect the four-layer architecture** (Landing → Raw → Curated → Curated History)
9. **Implement proper deduplication** using primary keys and hash-based change detection
10. **Handle schema evolution carefully** - schema evolution is disabled for data quality

### Security & Compliance
11. **Never hardcode credentials** - always use Databricks secret scopes via `get_secret()`
12. **Always anonymize PII data** before storing in raw layer using comprehensive_anonymization
13. **Never log sensitive information** (PII, credentials, API keys) in any log messages
14. **Use environment-based secret naming** for multi-environment deployments

### Performance
15. **Use broadcast joins** for small lookup tables (< 10MB)
16. **Cache intermediate results** when DataFrame is reused multiple times
17. **Avoid collect() operations** on large datasets - use limit() for sampling
18. **Implement proper partitioning** for large tables in Delta Lake
19. **Use coalesce() judiciously** - only for small result files

### Pipeline Design
20. **Implement graceful degradation** for missing data sources with proper fallbacks
21. **Use empty DataFrames with proper schema** as fallbacks on errors
22. **Log all errors with context** for debugging and monitoring
23. **Continue processing other datasets** when one fails to maximize pipeline resilience
24. **Validate upstream dependencies** before processing (e.g., check if landing files exist)

## Common Anti-Patterns to Avoid

### Data Processing
- Converting large DataFrames to pandas unnecessarily (use PySpark operations instead)
- Using collect() operations on large datasets (use limit() for sampling or aggregations)
- Not handling null/empty data gracefully (always check for nulls before operations)
- Ignoring schema validation (use Great Expectations and schema enforcement)

### Security
- Hard-coding credentials, file paths, or configuration values in source code
- Missing error handling around API calls (always wrap in try/except with logging)
- Skipping anonymization for PII data (required before raw layer storage)
- Logging sensitive information (PII, credentials, or API keys)

### Architecture
- Direct file writes instead of using Delta Lake (all persistent data must be Delta)
- Not respecting layer boundaries (e.g., transforming data in ingestion layer)
- Missing environment variable checks for sensitive credentials
- Using pandas for large-scale transformations (use PySpark for > 1M rows)

### Pipeline Design
- Failing entire pipeline when one table fails (implement graceful degradation)
- Not implementing proper retry logic for API calls
- Missing data quality checks before writing to curated layer
- Not tracking data lineage with technical metadata fields

## Security & Compliance

### PII Data Handling
- **Anonymization**: All customer feedback text is anonymized in the raw layer using `comprehensive_anonymization()`
  - Finnish name and address register lookup
  - Regex-based detection: emails, phone numbers, SSNs, IBANs, insurance numbers, registration plates
  - Configurable tagging (`<nimi>`) or replacement (`XXXXX`) modes
- **Storage**: PII data never stored in plain text after raw layer processing
- **Access Control**: Unity Catalog provides table-level and column-level access control

### Credential Management
- **Databricks Secret Scopes**: All credentials stored in environment-specific secret scopes
  - `sql-db-secrets`: SQL Server credentials (SQL_USER_{env}, SQL_PASSWORD_{env})
  - `api-secrets`: NLP API keys and URLs
  - `s3-secrets`: AWS S3 access credentials
- **Environment Isolation**: Separate secrets per environment (dev/test/qa/prod) using `{SECRET_NAME}_{env}` pattern
- **Local Development**: Fallback to `os.getenv()` when secret scopes unavailable
- **No Hardcoding**: Never commit credentials to git repositories

### API Security
- **Keys**: All API keys retrieved from Databricks secret scopes via `get_secret()` function
- **URLs**: API endpoints also stored in secret scopes for environment flexibility
- **Timeout Configuration**: All API calls have configurable timeouts (default 60s)

### Logging Best Practices
- **Never Log**: PII data, credentials, API keys, or customer identifiable information
- **Always Log**: Error context, row counts, processing timestamps, pipeline stage completion
- **Log Levels**: INFO for normal operations, WARNING for degraded state, ERROR for failures

### Data Quality & Governance
- **Great Expectations**: Validates data quality at curated layer entry point
- **DataHub Integration**: Metadata published for data discovery and lineage tracking
- **Audit Trail**: Curated History layer provides complete change history with timestamps

For detailed secret setup instructions, see [SECRETS_SETUP.md](SECRETS_SETUP.md).

This project prioritizes **data privacy**, **data quality**, **maintainability**, and **scalability** while following cloud-native best practices, lakehouse architecture principles, and the four-layer data model (Landing → Raw → Curated → Curated History).

## Additional Documentation
- [PIPELINE_ORCHESTRATION.md](PIPELINE_ORCHESTRATION.md) - Detailed pipeline execution flow and orchestration guide
- [SECRETS_SETUP.md](SECRETS_SETUP.md) - Complete guide for setting up Databricks secret scopes
- [DAB_INSTRUCTIONS.md](DAB_INSTRUCTIONS.md) - Databricks Asset Bundle deployment and configuration
