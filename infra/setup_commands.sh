# Enable APIs
gcloud services enable pubsub.googleapis.com
gcloud services enable dataflow.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable storage.googleapis.com

# Create Pub/Sub
gcloud pubsub topics create user-events

# Create BigQuery dataset
bq mk analytics_dataset

# Create GCS bucket
gsutil mb gs://your-bucket