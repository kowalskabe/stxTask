import argparse
import logging

import ijson
import pandas as pd
from more_itertools import grouper

"""
Skeleton for Squirro Delivery Hiring Coding Challenge
August 2021
"""

log = logging.getLogger(__name__)


class NYTimesSource(object):
    """
    A data loader plugin for the NY Times Data.
    """

    def __init__(self):
        pass

    def connect(self, inc_column=None, max_inc_value=None):
        # Ignore this method
        log.debug("Incremental Column: %r", inc_column)
        log.debug("Incremental Last Value: %r", max_inc_value)

    def disconnect(self):
        """Disconnect from the source."""
        # Nothing to do
        pass

    def getDataBatch(self, batch_size):
        """
        Generator - Get data from source on batches.

        :returns One list for each batch. Each of those is a list of
                 dictionaries with the defined rows.
        """

        def flatten(d, parent_key='', sep='.'):
            items = []
            for k, v in d.items():
                new_key = parent_key + sep + k if parent_key else k
                if isinstance(v, dict):
                    items.extend(flatten(v, new_key, sep=sep).items())
                elif isinstance(v, list):
                    for idx, value in enumerate(v):
                        items.extend(flatten(value, f'{new_key}.{idx}', sep=sep).items())
                else:
                    items.append((new_key, v))
            return dict(items)

        # TODO: implement - this dummy implementation returns one batch of data
        try:
            xls = pd.ExcelFile(config.get('reference_data_file'))
            df1 = pd.read_excel(xls, sheet_name=0, skiprows=lambda x: x in [0, 1], usecols='B:E')
            df2 = pd.read_excel(xls, sheet_name=1)
            merged_xls_df = pd.merge(left=df1, right=df2, how='left', left_on='Reference Id', right_on='Reference Id')
            merged_xls_df = merged_xls_df.drop_duplicates('Article Id', keep='last')
        except Exception as e:
            return (f"I can't xlsx file named 'reference_data_file'")

        self.doc_list = list()

        with open(config.get('api_response_file'), encoding='UTF-8') as json_file:
            docs = ijson.items(json_file, 'response.docs.item')
            for doc in docs:
                self.doc_list.append(flatten(doc))

        merged_xls_dict = merged_xls_df.to_dict('records')

        for i in range(len(self.doc_list)):
            for item in merged_xls_dict:
                if item['Article Id'].strip() == self.doc_list[i]['_id'].strip():
                    self.doc_list[i]['status'] = item['Status']
                    if item['Status'] == 'Reviewed':
                        self.doc_list[i]['date_completed'] = item['Date Completed']
                        self.doc_list[i]['reviewer'] = item['Reviewer']

        for item in grouper(batch_size, self.doc_list):
            result = list(filter(None, item))
            yield result

        # yield [
        #    {
        #        "headline.main": "The main headline",
        #        "_id": "1234",
        #    }
        # ]

    def getSchema(self):
        """
        Return the schema of the dataset
        :returns a List containing the names of the columns retrieved from the
        source
        """

        # TODO: Update the schema to reflect the item (row)
        if hasattr(self, 'doc_list'):
            schema_tpm = self.doc_list[0].keys()
        else:
            schema_tpm = []

        return schema_tpm


if __name__ == "__main__":
    config = {
        "api_response_file": "api_response.json",
        "reference_data_file": "reference_data.xlsx"
    }
    source = NYTimesSource()

    # This looks like an argparse dependency - but the Namespace class is just
    # a simple way to create an object holding attributes.
    source.args = argparse.Namespace(**config)

    for idx, batch in enumerate(source.getDataBatch(3)):

        print(f"{idx} Batch of {len(batch)} items")
        for item in batch:
            # Hint item["status"] and item.get("date_completed") should come from the
            # excel file
            print(f"{item['_id']} - {item['headline.main']}")
            print(f" --> {item['status']} - {item.get('date_completed')}")
