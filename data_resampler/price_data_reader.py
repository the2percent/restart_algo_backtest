import pandas as pd
import os

# def read_instrument_data(instrument_file_name):
#     folder_path = "/Users/piyush.potdukhe/Desktop/my_repos/restart/downloaded_instrument_data"
#     file_path = os.path.join(folder_path, instrument_file_name)
#     if os.path.exists(file_path):
#         return pd.read_parquet(file_path)
#     else:
#         return pd.DataFrame()

def read_instrument_data(folder_path, instrument_file_name):
    file_path = os.path.join(folder_path, instrument_file_name)
    if os.path.exists(file_path):
        return pd.read_parquet(file_path)
    else:
        return pd.DataFrame()