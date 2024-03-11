
import logging
import os
from unittest.mock import patch, MagicMock
import pandas as pd

logging.basicConfig(level='INFO')  # Use DEBUG, INFO, or WARNING
logger = logging.getLogger(__name__)

class MockEIA:

    def __init__(self):
        self.patcher = patch('boto3.client')
        self.mock_boto3_client = self.patcher.start()
        self.dummy_client = MagicMock()
        self.mock_boto3_client.return_value = self.dummy_client

        self.patcher_read_delimited_truth_data_file_from_S3 = patch('comstockpostproc.cbecs.CBECS.read_delimited_truth_data_file_from_S3')
        self.mock_read_delimited_truth_data_file_from_S3 = self.patcher_read_delimited_truth_data_file_from_S3.start()
        self.mock_read_delimited_truth_data_file_from_S3.side_effect = self.mock_read_delimited_truth_data_file_from_S3_action

        self.original_read_csv = pd.read_csv
        self.patcher__read_csv = patch('pandas.read_csv')
        self.mock__read_csv = self.patcher__read_csv.start()
        self.mock__read_csv.side_effect = self.mock__read_csv_action
        
        self.original_read_parquet = pd.read_parquet
        self.patcher__read_parquet = patch('pandas.read_parquet')
        self.mock__read_parquet = self.patcher__read_parquet.start()
        self.mock__read_parquet.side_effect = self.mock__read_parquet_action

    def mock__read_csv_action(self, *args ,**kwargs):
        logging.info('Mocking read_csv from EIA')
        path = args[0] 
        filePath = None
        mount_point = "./truth_data/v01/EIA/CBECS/"

        if "CBECS_2018_microdata.csv" in path:
            filePath = os.path.join(mount_point, "CBECS_2018_microdata.csv")
        elif "CBECS_2018_microdata_codebook.csv" in path:
            filePath = os.path.join(mount_point, "CBECS_2018_microdata_codebook.csv")
        elif "CBECS_2012_microdata.csv" in path:
            filePath = os.path.join(mount_point, "CBECS_2012_microdata.csv")
        elif "CBECS_2012_microdata_codebook.csv" in path:
            filePath = os.path.join(mount_point, "CBECS_2012_microdata_codebook.csv")

        if filePath is None:
            return self.original_read_csv(*args, **kwargs)
        return self.original_read_csv(filePath, **kwargs)

    def mock__read_parquet_action(self, *args, **kwargs):
        logging.info("read parquet from {}, {}".format(args, kwargs))
        logging.info('Mocking read_parquet from EIA')
        filePath = None
        path = args[0]
        if not filePath: return self.original_read_parquet(*args, **kwargs)
        return self.original_read_parquet(filePath, **kwargs)

    
    def mock_read_delimited_truth_data_file_from_S3_action(self, s3_file_path, delimiter):
        logging.info('reading from path: {} with delimiter {}'.format(s3_file_path, delimiter))
        return pd.DataFrame()

    def stop(self):
        self.patcher.stop()
        self.patcher_read_delimited_truth_data_file_from_S3.stop()
        self.patcher__read_csv.stop()
        self.patcher__read_parquet.stop()