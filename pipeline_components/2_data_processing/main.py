"""
The file should implement data processing pipeline.
It should clean the data and split it to train and test.
"""
from typing import List

import pandas as pd
from sklearn import datasets
import numpy as np
from scipy import stats

from src.data.processing.utils import rank_features, split_train_test
from src.utils import (Log, load_parquet, load_yaml, parse_arguments,
                       write_parquet)


def main(arguments_list: List = None):
    """Main method"""
    arguments = parse_arguments(arguments_list=arguments_list)
    config = load_yaml(arguments.config_path)
    main_config = load_yaml(arguments.main_config_path)

    Log.info(main_config.log_msg.process_data_start_msg)

    Log.info(main_config.log_msg.load_dataset_start_msg)
    data_df = load_parquet(path=main_config.path.raw_data_path)
    Log.info(main_config.log_msg.load_dataset_end_msg)

    data_df = data_df.dropna()
    data_df = data_df[data_df['transactionPricePerShare'].astype(float) != 0]
    # data_df = data_df[(np.abs(stats.zscore(data_df['transactionPricePerShare'].astype(float))) < data_df['transactionPricePerShare'].astype(float).quantile(0.1))]
    # print(data_df['transactionPricePerShare'].astype(float).describe())
    # data_df = data_df[data_df['transactionPricePerShare'].astype(int) != 0.00]

    Log.info(main_config.log_msg.save_data_as_parquet_start_msg)
    write_parquet(
        df=data_df,
        path=main_config.path.clean_data_path
    )
    Log.info(main_config.log_msg.save_data_as_parquet_end_msg)

    Log.info(main_config.log_msg.process_data_end_msg)


if __name__ == '__main__':
    main()
