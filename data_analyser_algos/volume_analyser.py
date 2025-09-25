# this file
# reads the output_files = "/Users/piyush.potdukhe/Desktop/my_repos/restart/generated_outputs" which has sub-folders daily, weekly, monthly
# for each file in each folder it reads the file and does below calculations on volume column
#   take all the rows, assign the "Volume_Percentile" column value as the percentile rank of volume column in the entire dataframe
#   calculate 20 period EMA of volume and save to new column "Volume_EMA_20"
#   calculate 50 period EMA of volume and save to new column "Volume_EMA_50"
#   calculate 200 period EMA of volume and save to new column "Volume_EMA_200"
#   restrict Volume_Percentile and Volume_EMA to be upto 2 decimal places
# save output to same file, overwriting it.

import pandas as pd
import os
from tqdm import tqdm
from data_resampler.price_data_reader import read_instrument_data

from all_constants.constants import output_folder, output_sub_folder_list

class VolumeAnalyzer:
    """Analyzes OHLC DataFrame based on volume indicators."""

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.calculate_volume_indicators()

    def calculate_volume_indicators(self):
        if self.df.empty:
            return
        
        # Calculate Volume Percentile Rank
        self.df['Volume_Percentile'] = (self.df['volume'].rank(pct=True) * 100).round(2)

        # add a column named "Volume_LTH" which is True if volume is the highest ever
        self.df['Volume_LTH'] = self.df['volume'] == self.df['volume'].max()

        # add a column named "Volume_Top_10_Percent" which is True if volume is in the top 10 percentile
        self.df['Volume_Top_10_Percent'] = self.df['Volume_Percentile'] > 90

        # add a column named "Volume_Last_5_Candles" which is True, if volume in the last 5 candles is Volume_LTH or Volume_Top_10_Percent
        self.df['Volume_Last_5_Candles'] = self.df['Volume_LTH'].rolling(window=5, min_periods=1).max().astype(bool) | \
                                        self.df['Volume_Top_10_Percent'].rolling(window=5, min_periods=1).max().astype(bool)

        # Calculate EMAs for volume
        for span in [20, 50, 200]:
            col_name = f"Volume_EMA_{span}"
            self.df[col_name] = self.df['volume'].ewm(span=span, adjust=False).mean().round(2)


if __name__ == "__main__":
    # now output_folder will have sub-folders daily, weekly, monthly which will have resampled data already
    # calculate volume indicators for each row in each file in each folder and save output to a new column in same file.
    freq_folders = {freq: os.path.join(output_folder, freq) for freq in output_sub_folder_list}
    for folder in freq_folders.values():
        files = [f for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f))]
        for filename in tqdm(files, desc=f"Processing files (Volume Analyser) in {os.path.relpath(folder)}"):
            file_path = os.path.join(folder, filename)
            df = read_instrument_data(folder, filename)
            if df.empty:
                continue
            analyzer = VolumeAnalyzer(df)
            out_path = os.path.join(folder, filename)  # overwrite same file
            analyzer.df.to_parquet(out_path, index=False)
