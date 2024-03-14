from unittest.mock import patch, MagicMock
import pandas as pd
import polars as pl
import os
from unittest.mock import patch
import logging

logging.basicConfig(level='INFO')  # Use DEBUG, INFO, or WARNING
logger = logging.getLogger(__name__)
class MockComStock:
    def __init__(self):
        self.patcher = patch('boto3.client')
        self.mock_boto3_client = self.patcher.start()
        self.dummy_client = MagicMock()
        self.mock_boto3_client.return_value = self.dummy_client
        
        self.patcher_athena_client = patch('buildstock_query.BuildStockQuery')
        self.mock_athena_client = self.patcher_athena_client.start()
        self.mock_athena_client.return_value = self.dummy_client

        # self.data = data

        # Mocking the S3 methods since the S3 utilitimix is base class
        # we are not able to mock the class directly
        # more information: https://stackoverflow.com/questions/38928243/patching-a-parent-class
        
        self.patcher_isfile_on_S3 = patch('comstockpostproc.comstock.ComStock.isfile_on_S3')
        self.mock_isfile_on_S3 = self.patcher_isfile_on_S3.start()
        self.mock_isfile_on_S3.side_effect = self.mock_isfile_on_S3_action

        self.patcher_upload_data_to_S3 = patch('comstockpostproc.comstock.ComStock.upload_data_to_S3')
        self.mock_upload_data_to_S3 = self.patcher_upload_data_to_S3.start()
        self.mock_upload_data_to_S3.side_effect = self.mock_upload_data_to_S3_action

        self.patcher_read_delimited_truth_data_file_from_S3 = patch('comstockpostproc.comstock.ComStock.read_delimited_truth_data_file_from_S3')
        self.mock_read_delimited_truth_data_file_from_S3 = self.patcher_read_delimited_truth_data_file_from_S3.start()
        self.mock_read_delimited_truth_data_file_from_S3.side_effect = self.mock_read_delimited_truth_data_file_from_S3_action

        self.polar_read_csv = pl.read_csv 
        self.patcher_polar_read_csv = patch('polars.read_csv')
        self.mock_polar_read_csv = self.patcher_polar_read_csv.start()
        self.mock_polar_read_csv.side_effect = self.mock__polars_read_csv_action
        
        self.original_read_parquet = pd.read_parquet
        self.patcher__read_parquet = patch('pandas.read_parquet')
        self.mock__read_parquet = self.patcher__read_parquet.start()
        self.mock__read_parquet.side_effect = self.mock__read_parquet_action

        self.original_check_file_exists = os.path.exists
        self.patcher_check_file_exists = patch('os.path.exists')
        self.mock_check_file_exists = self.patcher_check_file_exists.start()
        self.mock_check_file_exists.side_effect = self.mock_check_file_exists_action

        self.pandas_read_csv = pd.read_csv
        self.patcher_pandas_read_csv = patch('pandas.read_csv')
        self.mock_pandas_read_csv = self.patcher_pandas_read_csv.start()
        self.mock_pandas_read_csv.side_effect = self.mock__pandas_read_csv_action

    def mock_upload_data_to_S3_action(self, file_path, s3_file_path):
        logging.info('Uploading {}...'.format(file_path))

    def mock_read_delimited_truth_data_file_from_S3_action(self, s3_file_path, delimiter):
        logging.info('reading from path: {} with delimiter {}'.format(s3_file_path, delimiter))
        local_path = None
        if "EJSCREEN" in s3_file_path:
            local_path = "./truth_data/v01/EPA/EJSCREEN/EJSCREEN_Tract_2020_USPR.csv"
        elif "1.0-communities.csv" in s3_file_path:
            local_path = "./truth_data/v01/EPA/CEJST/1.0-communities.csv"
        elif "eGRID/egrid_emissions_2019.csv" in s3_file_path:
            local_path = "./truth_data/v01/EPA/eGRID/egrid_emissions_2019.csv"

        if not local_path:
            return pd.DataFrame() 

        try:
            return pd.read_csv(local_path, delimiter=delimiter)
        except UnicodeDecodeError:
            return pd.read_csv(local_path, delimiter=delimiter, encoding='latin-1')

    def mock_isfile_on_S3_action(self, bucket, file_path):
        logging.info('Mocking isfile_on_S3')
        return True
    
    def mock__polars_read_csv_action(self, *args, **kwargs):
        logging.info('Mocking read_csv from ComStock')
        filePath = None
        path = args[0]
        logging.info('path: {}'.format(path))
        if "EJSCREEN" in path:
            filePath = "./truth_data/v01/EPA/EJSCREEN/EJSCREEN_Tract_2020_USPR.csv"
        elif "1.0-communities.csv" in path:
            filePath = "./truth_data/v01/EPA/CEJST/1.0-communities.csv"
        elif "com_os340_stds_030_10k_test_1/buildstock.csv" in path:
            filePath = "./s3/comstock-core/test/com_os340_stds_030_10k_test_1/com_os340_stds_030_10k_test_1/buildstock.csv"
        elif "comstock_data/ami_comparison/buildstock.csv" in path:
            filePath = "./s3/eulp/comstock_core/ami_comparison/ami_comparison/buildstock_csv/buildstock.csv"

        if not filePath: 
            return self.polar_read_csv(*args, **kwargs)
        return self.polar_read_csv(filePath, **kwargs)
    
    def mock__read_parquet_action(self, *args, **kwargs):
        logging.info("read parquet from {}, {}".format(args, kwargs))
        logging.info('Mocking read_parquet from ComStock')
        filePath = None
        path = args[0]
        if path == "s3://comstock-core/test/com_os340_stds_030_10k_test_1/com_os340_stds_030_10k_test_1/baseline/results_up00.parquet":
            filePath = "./s3/comstock-core/test/com_os340_stds_030_10k_test_1/com_os340_stds_030_10k_test_1/baseline/results_up00.parquet"
        elif path == "s3://oedi-data-lake/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2021/resstock_amy2018_release_1/metadata/metadata.parquet":
            filePath = "./s3/end-use-load-profiles-for-us-building-stock/2021/resstock_amy2018_release_1/metadata/metadata.parquet"
        elif path == "s3://eulp/comstock_core/ami_comparison/ami_comparison/baseline/results_up00.parquet":
            filePath = "./s3/eulp/comstock_core/ami_comparison/ami_comparison/baseline/results_up00.parquet"
        if not filePath:
            return self.original_read_parquet(*args, **kwargs)
        return self.original_read_parquet(filePath, **kwargs)
    
    def mock_check_file_exists_action(self, file_path):
        local_path = None
        if "comstock_data/com_os340_stds_030_10k_test_1/buildstock.csv" in file_path:
            local_path = "./s3/comstock-core/test/com_os340_stds_030_10k_test_1/com_os340_stds_030_10k_test_1/buildstock.csv"
        elif "/truth_data/v01/1.0-communities.csv" in file_path:
            local_path = "./truth_data/v01/EPA/CEJST/1.0-communities.csv"
        elif "/truth_data/v01/egrid_emissions_2019.csv" in file_path:
            local_path = "./truth_data/v01/EPA/eGRID/egrid_emissions_2019.csv"
        elif "/truth_data/v01/EJSCREEN/EJSCREEN_Tract_2020_USPR.csv" in file_path:
            local_path = "./truth_data/v01/EPA/EJSCREEN/EJSCREEN_Tract_2020_USPR.csv"
        elif "ami_comparison/ami_comparison/baseline/results_up00.parquet" in file_path:
            local_path = "./s3/eulp/comstock_core/ami_comparison/ami_comparison/baseline/results_up00.parquet"
        elif "comstock_data/ami_comparison/buildstock.csv" in file_path:
            # raise Exception
            local_path = "./s3/eulp/comstock_core/ami_comparison/ami_comparison/buildstock_csv/buildstock.csv"
        logging.info('Mocking check_file_exists for file path {}, existed {}, local_path: {}'.format(file_path, 
                        self.original_check_file_exists(file_path), local_path))
        if local_path: logging.info('local path exists: {}'.format(os.path.exists(local_path)))
        if not local_path: return self.original_check_file_exists(file_path)
        return self.original_check_file_exists(local_path)

    def mock__pandas_read_csv_action(self, *args ,**kwargs):
        logging.info('Mocking read_csv from ComStock')
        logging.info(args, kwargs)
        local_path = None
        path = args[0]
        if "com_os340_stds_030_10k_test_1/buildstock.csv" in path:
            local_path = "./s3/comstock-core/test/com_os340_stds_030_10k_test_1/com_os340_stds_030_10k_test_1/buildstock.csv"
        if not local_path:
            return self.pandas_read_csv(*args, **kwargs)
        return self.pandas_read_csv(local_path, **kwargs)

    def stop(self):
        self.patcher.stop()
        self.patcher_isfile_on_S3.stop()
        self.patcher_upload_data_to_S3.stop()
        self.patcher_read_delimited_truth_data_file_from_S3.stop()
        self.patcher_polar_read_csv.stop()
        self.patcher__read_parquet.stop()
        self.patcher_check_file_exists.stop()
        self.patcher_pandas_read_csv.stop()
        self.patcher_athena_client.stop()
