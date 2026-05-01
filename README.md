#  Real-Time Event Analytics Pipeline (GCP)

This project demonstrates a **production-style real-time data engineering pipeline** built using Google Cloud Platform services.

It simulates an e-commerce platform generating live user activity and processes it into real-time analytics.

---

##  Problem Statement

Modern applications require **real-time insights** such as:

 Active users
 Product popularity
 Conversion rates

This project builds a pipeline to process **live streaming events** and generate analytics in near real-time.

---

##  Architecture

```
Cloud Run (Event Producer)
        ↓
Pub/Sub (Event Streaming)
        ↓
Dataflow (Apache Beam - Streaming Processing)
        ↓
BigQuery (Analytics Warehouse)
        ↓
Looker Studio (Dashboard)
```

---

##  Tech Stack

* Google Cloud Pub/Sub → Event ingestion
* Google Cloud Dataflow → Stream processing
* Google BigQuery → Data warehouse
* Google Cloud Run → Event producer
* Apache Beam → Pipeline framework

---

##  Event Schema

Each event represents a user action:

```json
{
  "user_id": "U123",
  "event_type": "click | view | purchase",
  "product_id": "P456",
  "timestamp": 1710000000
}
```

---

##  Pipeline Features

* Real-time event ingestion
* JSON parsing and validation
* Windowed aggregations (events per minute)
* Scalable streaming pipeline
* Continuous data ingestion into BigQuery

---

##  Project Structure

```
real-time-analytics-pipeline/
│
├── producer/          # Cloud Run event generator
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
│
├── dataflow/          # Apache Beam pipeline
│   ├── pipeline.py
│   └── requirements.txt
│
├── infra/             # Setup scripts
│   └── setup_commands.sh
│
├── README.md
└── .gitignore
```

---

##  Setup Instructions

### 1️ Clone Repository

```
git clone <your-repo-url>
cd real-time-analytics-pipeline
```

---

### 2️ Enable GCP Services

```
gcloud services enable pubsub.googleapis.com
gcloud services enable dataflow.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable storage.googleapis.com
```

---

### 3️ Create Infrastructure

```
bash infra/setup_commands.sh
```

---

### 4️ Run Dataflow Pipeline

```
python dataflow/pipeline.py \
  --runner=DataflowRunner \
  --project=<your-project-id> \
  --region=us-central1 \
  --temp_location=gs://<your-bucket>/temp
```

---

### 5 Deploy Event Producer

```
gcloud run deploy event-producer \
  --source ./producer \
  --region us-central1 \
  --allow-unauthenticated
```

---

##  Output

* Streaming data stored in BigQuery
* Aggregated metrics table
* Ready for visualization in Looker Studio

---

##  Key Concepts Demonstrated

* Stream processing
* Event-driven architecture
* Windowing (time-based aggregation)
* Distributed data pipelines
* Cloud-native data engineering

---

##  Challenges & Considerations

* Handling invalid JSON events
* Managing streaming latency
* Ensuring scalability of pipeline
* Cost optimization in BigQuery

---

##  Future Improvements

* Session windowing
* Late data handling (watermarks)
* Deduplication (idempotency)
* Real-time alerting system
* CI/CD pipeline integration

---

##  Author

Built as part of hands-on learning in Data Engineering.
