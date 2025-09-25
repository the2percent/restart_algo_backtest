import pandas as pd
import os
from tqdm import tqdm
from data_resampler.price_data_reader import read_instrument_data

class RSIAnalyzer:
    """Calculates RSI (Relative Strength Index) for provided OHLC DataFrame."""

    def __init__(self, df: pd.DataFrame, period=14):
        self.df = df.copy()
        self.period = period
        self.calculate_rsi()

    # def calculate_rsi(self): # calculate using simple moving average (SMA)
    #     delta = self.df['close'].diff()
    #     gain = (delta.where(delta > 0, 0)).rolling(self.period, min_periods=self.period).mean()
    #     loss = (-delta.where(delta < 0, 0)).rolling(self.period, min_periods=self.period).mean()
    #     rs = gain / loss
    #     self.df['RSI'] = (100 - (100 / (1 + rs))).clip(0,100)

    def calculate_rsi(self): # calculate using exponential moving average (EMA) - Wilder's method
        delta = self.df['close'].diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.ewm(alpha=1/self.period, min_periods=self.period).mean()
        avg_loss = loss.ewm(alpha=1/self.period, min_periods=self.period).mean()
        rs = avg_gain / avg_loss
        self.df['RSI'] = 100 - (100 / (1 + rs))

if __name__ == "__main__":
    input_folder = "/Users/piyush.potdukhe/Desktop/my_repos/restart/downloaded_instrument_data"
    output_folder = "/Users/piyush.potdukhe/Desktop/my_repos/restart/generated_outputs"
    
    # now output_folder will have sub-folders daily, weekly, monthly which will have resampled data already
    # calculate RSI for each row in each file in each folder and save output to a new column in same file.
    freq_folders = {freq: os.path.join(output_folder, freq) for freq in ["daily", "weekly", "monthly"]}
    for folder in freq_folders.values():
        files = [f for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f))]
        for filename in tqdm(files, desc=f"Processing files (RSI) in {os.path.relpath(folder)}"):
            file_path = os.path.join(folder, filename)
            df = read_instrument_data(folder, filename)
            if df.empty:
                continue
            analyzer = RSIAnalyzer(df, period=14)
            out_path = os.path.join(folder, filename)  # overwrite same file
            analyzer.df.to_parquet(out_path, index=False)