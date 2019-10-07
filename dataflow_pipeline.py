# Pipeline using Google Cloud Dataflow
# Tsung-Chin Han
# Date: Oct 6th, 2019

"""`dataflow_pipeline.py` is a batch processing example.
It uses google cloud dataflow pipeline that read a csv file from Google Cloud Stroage
and write the contents to a BigQuery table.

This project writes two tables in BigQuery:
1. Firsrt process is to write the orignial contents to BigQuery table.
2. Intermidate process is then performing groupby and custom count function
   and write its trasnformation to BigQuery with another table.
"""

### pacakge dependencies
from __future__ import absolute_import
import argparse
import logging
import re
import csv
import sys
import os
# apache_beam
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json


class DataProcess:
    """A helper class that includes the logic to translate the csv file into a
    format BigQuery will be acceptable.
    """

    def __init__(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))

        # Form the first BigQuery table schema
        self.schema_str = ''
        # here read the output schema from a json file in resources folder.
        # This is used to specify the types of data we are writing to BigQuery.
        schema_file = os.path.join(dir_path, 'resources', 'schema.json')
        with open(schema_file) as f:
            data = f.read()
            # Wrapping the schema in fields is required for the BigQuery API.
            self.schema_str = '{"fields": ' + data + '}'

        # Form the second BigQuery table schema
        self.schema_str_2 = ''
        # here read the output schema from a json file in resources folder.
        # This is used to specify the types of data we are writing to BigQuery.
        schema_file = os.path.join(dir_path, 'resources', 'schema_transformed.json')
        with open(schema_file) as f:
            data = f.read()
            # Wrapping the schema in fields is required for the BigQuery API.
            self.schema_str_2 = '{"fields": ' + data + '}'

    def parse_method(self, string_input):
        """This method translates a single line of comma separated values to a
        dictionary which can be loaded into BigQuery.

        Args:
            string_input: A comma separated list of values in the form
        Returns:
            A dict mapping BigQuery column names as keys to the corresponding value
            parsed from string_input. The data is not transformed and remains in
            the same format as the CSV file.``

        output form:
              {
              'field name_1': 'value_1',
              'field name_2': 'value_2',
              ...
              }
        """

        # Strip the return and quote characters.
        schema = parse_table_schema_from_json(self.schema_str)
        schema_2 = parse_table_schema_from_json(self.schema_str_2)
        # field mapping
        field_map = [f for f in schema.fields]

        # encoding
        reload(sys)
        sys.setdefaultencoding( "utf-8" )

        # use csv Reader that deal with quoted strings.
        reader = csv.reader(string_input.split('\n'))
        for csv_row in reader:
            values = [x.decode('utf8') for x in csv_row]

            # store in dictionary. iterate over the values from the csv file
            row = {}
            i = 0
            for value in values:
                # dictionary to get the value
                row[field_map[i].name] = value
                i += 1

            return row


def run(argv=None):
    """The main function that builds the pipeline and runs a batch processing.
    """
    # add specific command line arguments we expect
    parser = argparse.ArgumentParser()

    # Final stage of the pipeline. Define the destination of the data.
    # Define the input file to read and the output table to import in BigQuery.
    parser.add_argument(
        '--input',
        dest='input',
        required=False,
        help='Input file to read. A local file or '
        'a file in a Google Storage Bucket.',
        default='gs://takehom-data-k/AB_NYC_2019.csv')

    # first, we create the dataset from the command: bq mk AB_NYC_2019
    # This defaults to the project dataset in the BigQuery.
    # Form: dataset.table
    parser.add_argument('--output',
                        dest='output',
                        required=False,
                        help='Output BigQuery table to import results to.',
                        default='AB_NYC_2019.AB_NYC_2019')

    # parse args from the command line
    known_args, pipeline_args = parser.parse_known_args(argv)

    # use the class DataProcess to process the logic
    # and transform the file into a BigQuery table
    data_process = DataProcess()

    # Define output schema from json file
    schema = parse_table_schema_from_json(data_process.schema_str) # output schema for raw data
    schema_2 = parse_table_schema_from_json(data_process.schema_str_2) # output schema for transformed data

    # !
     """Initiate the pipeline:
     using the pipeline arguments passed in from the commnd line.
     example:
         python dataflow_pipeline.py --project=$PROJECT\
         --runner=DataflowRunner\
         --staging_location=gs://$PROJECT/temp
         --temp_location gs://$PROJECT/temp\
         --input gs://$PROJECT/AB_NYC_2019.csv\
         --save_main_session
     """
    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:

        # read the soucre file of the pipeline
        # skip the first row (header) and starts with the lines read from the file
        lines = p | 'Read from a csv File' >> beam.io.ReadFromText(known_args.input, skip_header_lines=1)

        """1. First pipeline flow for raw data input and output.
        """
        # the pipeline translate from the csv file single row
        # input a string to a dictionary object that is acceptable by BigQuery
        # this is done by the defined helper function, which run parallel
        # using input from previous stage
        csv_lines = (lines
        |'String To BigQuery Row' >>
        beam.Map(lambda s: data_process.parse_method(s)))

        # output stage is the importing stage to BigQuery table, taking the previous
        # stage of the pipline
        original_output = (csv_lines
        |'Write to BigQuery - Raw Data' >> beam.io.Write(
            beam.io.BigQuerySink(
                # The table name is a required argument for the BigQuery sink
                # this is the value passed in from the command line
                known_args.output+'_raw',
                # The schema is from our schema file:
                # with the form fieldName:fieldType
                schema=schema,
                # creates the table in BigQuery
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                # before writing in, deletes all data in the BigQuery table
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))

        """2. Scenod pipeline flow that performs group by and counts
              with two fields information. input and output.
        """
        # helper function to count the number of listing by each neighbourhood
        # return into dictionary object and unicode string format
        # our schema file handles assinged data type when importing in BigQuery
        def count_listings(neighbourhood_listings):
            (neighbourhood, listings) = neighbourhood_listings
            n = str(sum(listings))
            return {u'neighbourhood': neighbourhood, u'count_listings': unicode(n)}

        # this stage of pipline is to to take the previous pipeline processed dictionary object
        # then projected to the fields we are interested
        # then create a key value pair for further aggregation
        # then perform group by on 'neighborhood' field with the filed 'calculated_host_listings_count'
        # after group by, we leverage the helper funtion to count the host_listing
        # this is by summing up values in each group by filed 'neighborhood'
        transformed_lines = (csv_lines
        |'Projected to desired fields' >>
        beam.Map(lambda row: {f: row[f] for f in ['neighbourhood', 'calculated_host_listings_count']})
        | 'Key-Value pair' >>
        beam.Map(lambda row: (row['neighbourhood'], int(row['calculated_host_listings_count'])))
        | 'Group' >>
        beam.GroupByKey()
        | 'Count' >>
        beam.Map(count_listings))

        # second output stage is the importing stage to BigQuery table, taking the previous
        # stage of the pipline for output that is performed aggregation and transformation
        transformed_output = (transformed_lines
        | 'Write to BigQuery - Transformed Data' >> beam.io.Write(
            beam.io.BigQuerySink(
                # The table name is a required argument for the BigQuery sink
                # this is the value passed in from the command line
                known_args.output+'_transform',
                # The schema is from our schema file:
                # with the form fieldName:fieldType
                schema=schema_2,
                # creates the table in BigQuery
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                # before writing in, deletes all data in the BigQuery table
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
