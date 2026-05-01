# Real-Time Event Analytics Pipeline

This project demonstrates a real-time streaming data pipeline using Google Cloud Platform.

##  Architecture

Cloud Run (Event Producer)
→ Pub/Sub
→ Dataflow (Apache Beam)
→ BigQuery
→ Looker Studio

##  Tech Stack

- Pub/Sub
- Dataflow (Apache Beam)
- BigQuery
- Cloud Run

##  Features

- Real-time event ingestion
- Streaming transformations
- Windowed aggregations (events per minute)
- Data warehouse storage
- Dashboard-ready output

##  Event Schema

```json
{
  "user_id": "U123",
  "event_type": "click",
  "product_id": "P456",
  "timestamp": 1710000000
}