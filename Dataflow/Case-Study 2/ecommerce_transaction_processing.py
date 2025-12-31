def run_pipeline():
    
    # import modules
    import apache_beam as beam
    import logging
    from apache_beam.options.pipeline_options import PipelineOptions
    from datetime import datetime
    
    # define pipeline options
    beam_options = PipelineOptions(
        runner="DataflowRunner",
        project="poised-defender-452509-e8",
        region="us-east1",
        job_name="usecase1",
        temp_location="gs://bucket_3_sdk_732025/temp/",
        staging_location="gs://bucket_3_sdk_732025/stg/"
    )
    
    # variables 
    csv_source_path = "gs://bucket_3_sdk_732025/transactions.csv"
    sink_invalid_path = "gs://bucket_3_sdk_732025/invalid/transactions_invalid"
    table_id = "poised-defender-452509-e8.insight_ds.transactions_valid"
    BQ_SCHEMA = "transaction_id:STRING,user_id:INT64,product_id:STRING,quantity:INT64,price:FLOAT64,timestamp:TIMESTAMP,total_sales:FLOAT64"
    
    class ValidateAndTransform(beam.DoFn):
        def process(self, element):
            fields = element.split(",")
            
            try:
                # extract fields
                transaction_id = fields[0]
                user_id = fields[1]
                product_id = fields[2]
                quantity = int(fields[3])
                price = float(fields[4])
                timestamp = fields[5]
                
                # validate fields
                if not user_id or not product_id:
                    raise ValueError("Missing user_id or product_id")
                if quantity <= 0:
                    raise ValueError("Invalid quantity: must be greater than 0")
                if price <= 0:
                    raise ValueError("Invalid price: must be greater than 0")
                    
                # calculate total sales
                total_sales = quantity * price
                
                # return as clean dict
                yield {
                    "transaction_id": transaction_id,
                    "user_id": user_id,
                    "product_id": product_id,
                    "quantity": quantity,
                    "price": price,
                    "timestamp": timestamp,
                    "total_sales": total_sales
                }
                
            except Exception as e:
                logging.error(f"Error processing transaction: {str(e)}") 
                
                # Sending invalid record to side output
                yield beam.pvalue.TaggedOutput('invalid_records', element)
            
    
    with beam.Pipeline(options=beam_options) as pipeline:
        
        pc1 = pipeline | "read from source" >> beam.io.ReadFromText(csv_source_path, skip_header_lines=1)
        pc_valid, pc_inv = pc1 | "validate & transform" >> beam.ParDo(ValidateAndTransform()).with_outputs("invalid_records", main="valid_records")

        # Write valid records to BigQuery
        pc_valid | "write to bq" >> beam.io.WriteToBigQuery(
            table=table_id,
            schema=BQ_SCHEMA,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            custom_gcs_temp_location="gs://bucket_3_sdk_732025/stage/"
        )
        
        # Write invalid records to GCS bucket as JSON
        pc_inv | "write to gcs" >> beam.io.WriteToText(sink_invalid_path, file_name_suffix=".json")
        
        # Set logging level to INFO
        logging.getLogger().setLevel(logging.INFO)
        
if __name__ == "__main__":
    run_pipeline()
