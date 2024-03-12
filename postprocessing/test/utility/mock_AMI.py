from unittest.mock import patch, MagicMock
import pandas as pd
import polars as pl
import os
from unittest.mock import patch
import logging

logging.basicConfig(level='INFO')  # Use DEBUG, INFO, or WARNING
logger = logging.getLogger(__name__)
class MockAMI:

    def __init__(self):
        self.original_pd_read_parquet = pd.read_parquet
        self.pacther__pd_read_parquet = patch('pandas.read_parquet')
        self.mock__pd_read_parquet = self.pacther__pd_read_parquet.start()
        self.mock__pd_read_parquet.side_effect = self.mock__pd_read_parquet_action

    def mock__pd_read_parquet_action(self, *args, **kwargs):
        logging.info('Mocking read_parquet from MockAMI')
        logging.info(args, kwargs)
        path = args[0]
        local_path = None
        if "s3://eulp/comstock_core/ami_comparison/ami_comparison/baseline/results_up00.parquet" == path:
            local_path = "./s3/eulp/comstock_core/ami_comparison/ami_comparison/baseline/results_up00.parquet"
        if not local_path:
            return self.original_pd_read_parquet(*args, **kwargs)
        return self.original_pd_read_parquet(local_path, **kwargs)
    
    def stop(self):
        self.pacther__pd_read_parquet.stop()