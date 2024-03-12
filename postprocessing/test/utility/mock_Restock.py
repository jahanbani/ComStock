from unittest.mock import patch, MagicMock
import pandas as pd
import polars as pl
import os
from unittest.mock import patch
import logging

logging.basicConfig(level='INFO')  # Use DEBUG, INFO, or WARNING
logger = logging.getLogger(__name__)


class MockResStock:

    def __init__(self):
        self.patcher = patch('boto3.client')
        self.mock_boto3_client = self.patcher.start()
        self.dummy_client = MagicMock()
        self.mock_boto3_client.return_value = self.dummy_client

        self.patcher_read_delimited_truth_data_file_from_S3 = patch('comstockpostproc.resstock.ResStock.read_delimited_truth_data_file_from_S3')
        self.mock_read_delimited_truth_data_file_from_S3 = self.patcher_read_delimited_truth_data_file_from_S3.start()
        self.mock_read_delimited_truth_data_file_from_S3.side_effect = self.mock_read_delimited_truth_data_file_from_S3_action
    
    def mock_read_delimited_truth_data_file_from_S3_action(self, s3_file_path, delimiter):
        logging.info('reading from path: {} with delimiter {} from mock Resstock'.format(s3_file_path, delimiter))
        local_path = None
        if "eGRID/egrid_emissions_2019.csv" in s3_file_path:
            local_path = "./truth_data/v01/EPA/eGRID/egrid_emissions_2019.csv"
        if not local_path:
            return pd.read_csv(s3_file_path, delimiter=delimiter)
        return pd.read_csv(local_path, delimiter=delimiter)

    def stop(self):
        self.patcher.stop()
        pass
