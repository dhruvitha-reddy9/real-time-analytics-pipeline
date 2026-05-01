import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json

PROJECT_ID = "your-project-id"
DATASET = "analytics_dataset"
TABLE = "event_counts"

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
        temp_location="gs://your-bucket/temp"
    )

    with beam.Pipeline(options=options) as p:

        events = (
            p
            | "Read PubSub" >> beam.io.ReadFromPubSub(
                topic=f"projects/{PROJECT_ID}/topics/user-events"
            )
            | "Parse JSON" >> beam.ParDo(ParseEvent())
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

        counts | beam.io.WriteToBigQuery(
            table=f"{PROJECT_ID}:{DATASET}.{TABLE}",
            schema="event_type:STRING, count:INTEGER",
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )


if __name__ == "__main__":
    run()