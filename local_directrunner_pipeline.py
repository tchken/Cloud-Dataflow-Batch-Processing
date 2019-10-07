

from __future__ import absolute_import
import argparse
import logging
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

##
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json

##
import csv
import os


class DataIngestion:
    """A helper class which contains the logic to translate the file into
    a format BigQuery will accept."""

    def __init__(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        self.schema_str = ''
        # Here we read the output schema from a json file.  This is used to specify the types
        # of data we are writing to BigQuery.
        schema_file = os.path.join(dir_path, 'resources', 'schema.json')
        with open(schema_file) \
                as f:
            data = f.read()
            # Wrapping the schema in fields is required for the BigQuery API.
            self.schema_str = '{"fields": ' + data + '}'

    def parse_method(self, string_input):
        """This method translates a single line of comma separated values to a
    dictionary which can be loaded into BigQuery.
        Args:
            string_input: A comma separated list of values in the form of
            state_abbreviation,gender,year,name,count_of_babies,dataset_created_date
                example string_input: KS,F,1923,Dorothy,654,11/28/2016
        Returns:
            A dict mapping BigQuery column names as keys to the corresponding value
            parsed from string_input.  In this example, the data is not transformed, and
            remains in the same format as the CSV.  There are no date format transformations.

        """
        # Strip out return characters and quote characters.
        schema = parse_table_schema_from_json(self.schema_str)

        field_map = [f for f in schema.fields]

        #
        import sys
        reload(sys)
        sys.setdefaultencoding( "utf-8" )


        # Use a CSV Reader which can handle quoted strings etc.
        reader = csv.reader(string_input.split('\n'))
        for csv_row in reader:
            # values = [x.decode('utf8') for x in csv_row]
            values = [x.decode('utf8') for x in csv_row]

            row = {}
            i = 0
            for value in values:
                row[field_map[i].name] = value

                i += 1

            return row


def run(argv=None):
    """The main function which creates the pipeline and runs it."""

    parser = argparse.ArgumentParser()

    # Here we add some specific command line arguments we expect.
    # Specifically we have the input file to read and the output table to write.
    # This is the final stage of the pipeline, where we define the destination
    # of the data. In this case we are writing to BigQuery.
    parser.add_argument(
        '--input',
        dest='input',
        required=False,
        help='Input file to read. This can be a local file or '
        'a file in a Google Storage Bucket.',
        # This example file contains a total of only 10 lines.
        # Useful for developing on a small set of data.
        default='./data/AB_NYC_2019.csv')

    # This defaults to the lake dataset in your BigQuery project. You'll have
    # to create the lake dataset yourself using this command:
    # bq mk lake
    parser.add_argument('--output',
                        dest='output',
                        required=False,
                        help='Output BQ table to write results to.',
                        default='./output/result')

    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)

    # DataIngestion is a class we built in this script to hold the logic for
    # transforming the file into a BigQuery table.
    data_ingestion = DataIngestion()

    # Initiate the pipeline using the pipeline arguments passed in from the
    # command line. This includes information such as the project ID and
    # where Dataflow should store temp files.
    #
    schema = parse_table_schema_from_json(data_ingestion.schema_str)

    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:

        lines = p | 'Read from a File' >> beam.io.ReadFromText(known_args.input, skip_header_lines=1)

        csv_lines = (lines
        |'String To BigQuery Row' >>
        beam.Map(lambda s: data_ingestion.parse_method(s)))

        original_output = (csv_lines
        |'Write to BigQuery' >>
        beam.io.WriteToText(known_args.output+'1', file_name_suffix='.csv'))

        # Count the occurrences of each word.
        def count_listings(neighbourhood_listings):
            (neighbourhood, listings) = neighbourhood_listings
            n = str(sum(listings))
            return {u'neighbourhood': neighbourhood, u'count_listings': unicode(n)}


        #
        transformed_lines = (csv_lines
        |'Projected' >>
        beam.Map(lambda row: {f: row[f] for f in ['neighbourhood', 'calculated_host_listings_count']})
        | 'Key-Value' >>
        beam.Map(lambda row: (row['neighbourhood'], int(row['calculated_host_listings_count'])))
        | 'Group' >>
        beam.GroupByKey()
        | 'Count' >>
        beam.Map(count_listings))
        # | 'Format' >>
        # beam.Map(formatting))

        transformed_output = (transformed_lines
        | 'Write' >>
        beam.io.WriteToText(known_args.output+'2', file_name_suffix='.csv'))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
