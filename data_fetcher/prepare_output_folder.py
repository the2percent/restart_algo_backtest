# this file 
# 1. reads all the input files input_folder = "/Users/piyush.potdukhe/Desktop/my_repos/restart/downloaded_instrument_data"
# 2. prepares output folder if not exists output_folder = "/Users/piyush.potdukhe/Desktop/my_repos/restart/generated_outputs"
# 3. create a folder named "daily" inside the output_folder and save all the daily resampled data there
# 4. create a folder named "weekly" inside the output_folder and save all the weekly resampled data there
# 5. create a folder named "monthly" inside the output_folder and save all the monthly resampled data there

import os
import shutil
from tqdm import tqdm
from data_resampler.price_resampler import PriceResampler
from data_resampler.price_data_reader import read_instrument_data
from all_constants.constants import input_folder, output_folder

# input_folder = "/Users/piyush.potdukhe/Desktop/my_repos/restart/downloaded_instrument_data"
# output_folder = "/Users/piyush.potdukhe/Desktop/my_repos/restart/generated_outputs"
os.makedirs(output_folder, exist_ok=True)

# Define frequencies and their folders
freq_folders = {freq: os.path.join(output_folder, freq) for freq in ["daily", "weekly", "monthly"]}

# reset sub-folders (delete and create afresh)
for folder in freq_folders.values():
    if os.path.exists(folder):
        shutil.rmtree(folder)
    os.makedirs(folder, exist_ok=True)

# Process files
files = [f for f in os.listdir(input_folder) if f.endswith(".parquet")]  # or .csv, etc.
for filename in tqdm(files, desc="Processing files (Prepare Output Folder)"):
    input_df = read_instrument_data(input_folder, filename)
    if input_df.empty:
        continue

    price_resampler = PriceResampler(input_df)

    for freq, folder in freq_folders.items():
        df_out = getattr(price_resampler, f"to_{freq}")()  # dynamically call to_daily, to_weekly, to_monthly
        out_path = os.path.join(folder, filename)
        df_out.to_parquet(out_path, index=False)

print(f"Saved resampled data to {output_folder}")
# end of file