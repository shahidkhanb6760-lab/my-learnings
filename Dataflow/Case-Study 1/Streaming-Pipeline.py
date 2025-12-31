
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.metrics import Metrics
import logging
import google.cloud.logging

# Initialize Google Cloud Logging
logging_client = google.cloud.logging.Client()
logging_client.setup_logging()
logger = logging.getLogger('stream-pipeline-logger')

INPUT_TOPIC = "projects/model-arcadia-440702-q1/topics/my-topic"

# Logging helper function
def log_pipeline_step(step, message, level='INFO'):
    if level == 'INFO':
        logger.info(f"Step: {step}, Message: {message}")
    elif level == 'ERROR':
        logger.error(f"Step: {step}, Error: {message}")
    elif level == 'WARNING':
        logger.warning(f"Step: {step}, Warning: {message}")

# Define Beam options
beam_options = PipelineOptions(
    streaming=True,
    runner='DataflowRunner',
    project='model-arcadia-440702-q1',
    job_name='stream-dataflow-job-2',
    region='us-east1',
    temp_location='gs://test-bkt-123123123/temp23'
)

# Data parsing with validation and error handling
class ParseDataFn(beam.DoFn):
    def __init__(self):
        # Initialize metrics
        self.success_count = Metrics.counter('main', 'successful-records')
        self.failure_count = Metrics.counter('main', 'failed-records')
        self.processed_bytes = Metrics.distribution('main', 'processed-bytes')

    def process(self, element):
        try:
            # Decode element from bytes to string
            element = element.decode('utf-8')
            fields = element.split(",")

            HEADERS = ["id", "factor", "code", "time", "name"]

            # Validate the number of fields
            if len(fields) != len(HEADERS):
                self.failure_count.inc()
                log_pipeline_step('ParseData', f"Invalid record: {element}", level='WARNING')
                return  # Skip invalid record

            # Create a dictionary from the fields
            record = dict(zip(HEADERS, fields))

            # Validate the content of the fields
            if not record["id"] or not record["code"]:
                self.failure_count.inc()
                log_pipeline_step('ParseData', f"Missing required fields in record: {element}", level='WARNING')
                return  # Skip invalid record

            # Track the size of the processed element
            self.processed_bytes.update(len(element))

            self.success_count.inc()
            log_pipeline_step('ParseData', f"Successfully processed record: {record}", level='INFO')
            yield record

        except Exception as e:
            self.failure_count.inc()
            log_pipeline_step('ParseData', f"Error processing record: {element}, Error: {str(e)}", level='ERROR')

# Pipeline object
with beam.Pipeline(options=beam_options) as pipeline:
    (
        pipeline
        | "Read from Pub/Sub topic" >> beam.io.ReadFromPubSub(topic=INPUT_TOPIC)
        | "Parse and Validate Data" >> beam.ParDo(ParseDataFn())
        | "Write to BigQuery" >> beam.io.WriteToBigQuery(
            table="model-arcadia-440702-q1.silver_dataset.streamTable",
            schema='id:string, factor:string, code:string, time:string, name:string',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            custom_gcs_temp_location="gs://test-bkt-123123123/temp22/"
        )
    )
 