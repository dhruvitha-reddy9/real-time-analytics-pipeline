import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json

PROJECT_ID = "project-7c8b1e72-b1d9-4794-8e1"
BUCKET = "realtime-analytics-bucket-123"

class ParseEvent(beam.DoFn):
    def process(self, element):
        try:
            yield json.loads(element.decode("utf-8"))
        except:
            pass

def run():
    options = PipelineOptions(
        streaming=True,
        project=PROJECT_ID,
        region="us-central1",
        temp_location=f"gs://{BUCKET}/temp",
        staging_location=f"gs://{BUCKET}/staging",
        save_main_session=True
    )

    with beam.Pipeline(options=options) as p:

        events = (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(
                topic=f"projects/{PROJECT_ID}/topics/user-events"
            )
            | "ParseJSON" >> beam.ParDo(ParseEvent())
        )

        counts = (
            events
            | "Window" >> beam.WindowInto(beam.window.FixedWindows(60))
            | "Pair" >> beam.Map(lambda x: (x['event_type'], 1))
            | "Count" >> beam.CombinePerKey(sum)
            | "Format" >> beam.Map(lambda x: {
                "event_type": x[0],
                "count": x[1]
            })
        )

        counts | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
            table=f"{PROJECT_ID}:analytics_dataset.event_counts",
            schema="event_type:STRING, count:INTEGER",
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )

if __name__ == "__main__":
    run()