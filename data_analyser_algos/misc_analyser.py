# this file reads the output_files = "/Users/piyush.potdukhe/Desktop/my_repos/restart/generated_outputs" which has sub-folders daily, weekly, monthly
# for each folder (weekly, daily, monthly) it reads each file and get the last row which has precalculated values from other algos
# it then applies below filters, 
#   close price >= EMA_long
#   RSI > 60
#   EMA_short > EMA_long
#   sort by least distance of the close price from EMA_short
# finally it saves the output to a csv file locally: "/Users/piyush.potdukhe/Desktop/my_repos/restart/generated_outputs/misc_analyser.csv"

import pandas as pd
import os
from tqdm import tqdm
from data_resampler.price_data_reader import read_instrument_data

from all_constants.constants import output_folder, output_sub_folder_list

class MiscAnalyzer:
    """Analyzes OHLC DataFrame based on multiple indicators."""

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()

    def get_latest_row(self):
        if self.df.empty:
            return None
        return self.df.iloc[-1].to_dict()

    def apply_filters(self, latest_row):
        # Apply filters directly
        if not (latest_row['close'] >= latest_row['EMA_long'] and
                latest_row['RSI'] > 60 and
                latest_row['EMA_short'] > latest_row['EMA_long'] and
                latest_row['Cross_Type'] == 'Golden' and
                latest_row['Days_From_Cross'] <= 63 and # happened in last 2 months
                latest_row['Volume_Percentile'] > 70 and
                latest_row['Volume_Top_10_Percent'] == True and
                latest_row['Volume_Last_5_Candles'] == True
                ):
            return False
        return True
    
    def analyze(self):
        latest_row = self.get_latest_row()
        if not latest_row:
            return None

        if not self.apply_filters(latest_row):
            return None

        # Calculate distance from EMA_short in percentile terms upto 2 decimal places
        latest_row['Distance_From_EMA_Short'] = abs(latest_row['close'] - latest_row['EMA_short'])
        latest_row['Distance_From_EMA_Short_Percentile'] = round((latest_row['Distance_From_EMA_Short'] / latest_row['EMA_short'] * 100), 2)
        return latest_row

if __name__ == "__main__":
    all_latest_rows = []
    output_file_path = output_folder + "misc_analyser.csv"
    freq_folders = {freq: os.path.join(output_folder, freq) for freq in output_sub_folder_list}

    for folder in freq_folders.values():
        files = [f for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f))]
        for filename in tqdm(files, desc=f"Processing files (Misc Analyser) in {os.path.relpath(folder)}"):
            file_path = os.path.join(folder, filename)
            df = read_instrument_data(folder, filename)
            if df.empty:
                continue
            analyzer = MiscAnalyzer(df)
            latest_row = analyzer.analyze()
            if latest_row:
                # latest_row['Instrument'] = filename.replace('.parquet', '')  # Add instrument name
                all_latest_rows.append(latest_row)

    if all_latest_rows:
        result_df = pd.DataFrame(all_latest_rows)
        result_df = result_df.sort_values('Distance_From_EMA_Short_Percentile')
        result_df.to_csv(output_file_path, index=False)
        print(f"Saved analysis results to {output_file_path}")
    else:
        print("No data matched the criteria.")
