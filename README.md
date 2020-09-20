Fraud Detection Pipeline on GCP Dataflow
---------------------------------------------

# Highlights
- Uses a distributed ETL framework: Apache Beam on GCP Dataflow
- Streaming workload which is more complex than batch workload
  - Need to consider windowing on unbounded sequence and handling of late data (watermark)
  - Inference is real-time, model is deployed as part of workload
    - Latency improvement: saves a network round trip as compared to calling a prediction microservice
    - Less maintenance overhead: do not have to maintain a separate microservice and associated backend (e.g. database)
- Production grade
  - Messages in protobuf format → efficient serialization and deserialization
  - Python tests with pytest
  - Python static analyzer with mypy
  - Python linting using Black, standardize style guide when working in a team

# Coding and basic questions
All Dataflow transforms are packaged as `beam.PTransform` [components](https://beam.apache.org/documentation/programming-guide/#composite-transforms) for code reuse:
1. [Imputer](src/acme/fraudcop/etl/__init__.py)
1. [Transpose columns](src/acme/fraudcop/etl/__init__.py) to key-value rows
1. Experiment group [hash function](src/acme/fraudcop/experiments.py)
   1. Get a hash digest of the `user_id`, then get an integer checksum of the digest.
   1. SHA-256 recommended by [NIST](https://csrc.nist.gov/Projects/Hash-Functions/NIST-Policy-on-Hash-Functions)

# Inference pipeline
### DAG
Code: [src/acme/fraudcop/etl/inference_pipeline.py](src/acme/fraudcop/etl/inference_pipeline.py)

#### Stream transactions from PubSub
- Protobuf message: [src/acme/fraudcop/transactions/transaction.proto](src/acme/fraudcop/transactions/transaction.proto)
- Transform protobuf to Python NamedTuple for immutability and full range of Python types

#### Retrieve blacklists from Google Cloud Storage
- Card ids
- District ids
- Load data as iterator (not materialized as List because worker may OOM)
- Use set data structure to check for membership efficiently
- Check incoming transaction with blacklist - if there is a match, mark as fraud.

#### Assign experiment group
- Hash function: [src/acme/fraudcop/experiments.py](src/acme/fraudcop/experiments.py)
- Assign transaction to an experiment group with a float value `[0, 1]`
- Partition treatment groups by percent value e.g. A 30%, B 70%

#### Transform raw data into features
Preprocessing logic
- Imput null with zero or mean
- Frequency encoding of categorical variables
- Value’s count as a fraction of total count
- Date/time: week of day, week number, card age (from issued to transaction date)
- Standardization
- Ratios: transaction amount over average user daily spend

#### Make prediction
- Load model from Google Cloud Storage
- Model is reloaded in each streaming window (1 minute; configurable). This allows hot deployment of new models without taking down the pipeline.

#### Write results to BigQuery table
See [result table schema](#result-table-schema)

# Metrics pipeline
Log evaluation results to BigQuery
- Training code or evaluation job publishes metric to PubSub topic
- Pipeline reads metric from PubSub topic and writes to BigQuery table
- Code: [src/acme/fraudcop/etl/metrics_pipeline.py](src/acme/fraudcop/etl/metrics_pipeline.py)

# Design and trade-offs

#### Organize the codebase into separate repositories
- Dataflow pipelines
  - Push docker image to Google Container Registry (run on scheduler)
- Serving code
  - Publish private python package on Github / Gitlab / Bitbucket
  - Upload tarball to Google Cloud Storage 
- Training code
  - Push docker image to Google Container Registry (run on AI Platform)

#### Reason: Decoupling
- Each repo can be version controlled independently e.g. training repo can change but no changes to serving repo.
- Serving code can be embedded in any delivery channel besides Dataflow. In future if we also want to serve the model via microservice, we can add the serving code library to the microservice.
- Pre and post-processing logic is also encapsulated in the serving repo - following the Single Responsibility principle. 
- If we need to do preprocessing in other applications (e.g. Dataflow, training code), add the serving code library to the application.
- Deployment artifacts / targets are different for each repo, so the CI/CD pipelines are also different

#### Private Python dependencies on Dataflow
- However in Dataflow, I discovered that there are some issues in adding external packages that are not public on PyPI. These extra dependencies are distributed by Dataflow to every worker node for installation before executing the DAG.
- So I had no choice but to combine all repos into one, in order to get Dataflow to work.
- This means tight coupling of serving code to Dataflow pipeline which is less than ideal.
- This dependency problem may not be an issue on Beam Java API.

# Future work
If I have more time…
- Currently using a dummy model - train a model and upload model files to Google Cloud Storage
- After the Inference pipeline
  - Feedback pipeline to fill the ground truth (which may only happen days after the transaction) - Data Scientist can then use the dataset for training on new data
  - Online evaluation pipeline: publish metrics onto the Metrics pipeline
    - Detect distribution drift; whether the serving model is still well fitted on the data
- CI/CD on Cloud Build
  - Deploy Docker images to Google Container Registry which can then be run on a scheduler to deploy Dataflow pipelines 
- Test code coverage
- Implement distributed SMOTE algorithm on Dataflow to generate synthetic examples for an imbalanced class problem like fraud detection
  - My Spark implementation: https://github.com/seahrh/spark-util

# BigQuery tables

## Result table schema
```sql
create table if not exists fraudcop.transactions
(
  trans_id INT64 NOT NULL OPTIONS(description='transaction id')
  ,account_id INT64 OPTIONS(description='account id linked to the transaction')
  ,`date` INT64 OPTIONS(description='transaction date')
  ,type STRING OPTIONS(description='transaction type')
  ,operation STRING OPTIONS(description='transaction operation')
  ,amount FLOAT64 OPTIONS(description='transaction amount')
  ,balance FLOAT64 OPTIONS(description='transaction balance')
  ,k_symbol STRING OPTIONS(description='transaction k-symbol')
  ,bank STRING OPTIONS(description='bank linked to account id')
  ,account STRING OPTIONS(description='type of bank account')
  ,district_id INT64 OPTIONS(description='district id linked to account')
  ,acct_freq STRING OPTIONS(description='account frequency')
  ,a2 STRING OPTIONS(description='district masked field')
  ,a3 STRING OPTIONS(description='district masked field')
  ,a4 INT64 OPTIONS(description='district masked field')
  ,a5 INT64 OPTIONS(description='district masked field')
  ,a6 INT64 OPTIONS(description='district masked field')
  ,a7 INT64 OPTIONS(description='district masked field')
  ,a8 INT64 OPTIONS(description='district masked field')
  ,a9 INT64 OPTIONS(description='district masked field')
  ,a10 FLOAT64 OPTIONS(description='district masked field')
  ,a11 INT64 OPTIONS(description='district masked field')
  ,a12 FLOAT64 OPTIONS(description='district masked field')
  ,a13 FLOAT64 OPTIONS(description='district masked field')
  ,a14 INT64 OPTIONS(description='district masked field')
  ,a15 INT64 OPTIONS(description='district masked field')
  ,a16 INT64 OPTIONS(description='district masked field')
  ,disp_type STRING OPTIONS(description='disposition type')
  ,card_id INT64 OPTIONS(description='card id')
  ,card_type STRING OPTIONS(description='card type')
  ,card_issued STRING OPTIONS(description='datetime string when card was issued')
  ,loan_date INT64 OPTIONS(description='loan date')
  ,loan_amount INT64 OPTIONS(description='loan amount so far')
  ,loan_duration INT64 OPTIONS(description='loan duration so far')
  ,loan_payments FLOAT64 OPTIONS(description='loan payments so far')
  ,loan_status STRING OPTIONS(description='loan status')
  ,blacklist_is_fraud BOOL OPTIONS(description='marked as fraud because blacklisted')
  ,blacklist_reason STRING OPTIONS(description='reason for blacklist')
  ,experiment_group_hash FLOAT64 OPTIONS(description='experiment group hash value between 0..1')
  ,f_date_day_of_week INT64 OPTIONS(description='feature')
  ,f_date_week_number INT64 OPTIONS(description='feature')
  ,f_type FLOAT64 OPTIONS(description='feature')
  ,f_amount FLOAT64 OPTIONS(description='feature')
  ,f_amount_to_daily_spend FLOAT64 OPTIONS(description='feature')
  ,f_a4 FLOAT64 OPTIONS(description='feature')
  ,f_card_age FLOAT64 OPTIONS(description='feature')
  ,is_fraud_prediction FLOAT64 OPTIONS(description='prediction proba result')
  ,is_fraud bool OPTIONS(description='ground truth')
)
```
