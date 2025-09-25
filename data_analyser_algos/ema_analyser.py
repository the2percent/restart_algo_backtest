
import logging
import os
import numpy as np
import pandas as pd
from tqdm import tqdm

from data_pnl.pnl_calculator import PnLCalculator
from data_resampler.price_resampler import PriceResampler
from data_resampler.price_data_reader import read_instrument_data

from all_constants.constants import input_folder, output_folder, output_sub_folder_list

class EMACrossAnalyzer:
    """Calculates EMAs and identifies golden/death crosses within provided OHLC DataFrame."""

    def __init__(self, df: pd.DataFrame, span_short=0, span_long=0):
        self.df = df.sort_values('datetime').copy()

        # validate EMA spans
        if not (isinstance(span_short, int) and span_short > 0 and isinstance(span_long, int) and span_long > 0):
            raise ValueError("span_short and span_long must be positive integers")
        self.span_short = span_short
        self.span_long = span_long
        self.compute_ema()  # Auto compute EMAs on init
        self.update_crosses_in_df() # Auto update crosses in df on init
        

    def compute_ema(self):
        self.df['EMA_short'] = self.df['close'].ewm(span=self.span_short, adjust=False).mean()
        self.df['EMA_long'] = self.df['close'].ewm(span=self.span_long, adjust=False).mean()
        self.df['EMA_cross'] = str(self.span_short) + " x " + str(self.span_long) # for info in final output file

        return self.df

    def find_crosses(self) -> pd.DataFrame:
        ema_short = self.df['EMA_short']
        ema_long = self.df['EMA_long']
        cross_up = (ema_short > ema_long) & (ema_short.shift(1) <= ema_long.shift(1))
        cross_down = (ema_short < ema_long) & (ema_short.shift(1) >= ema_long.shift(1))
        cross_type = np.where(cross_up, 'Golden', np.where(cross_down, 'Death', None))
        crosses = self.df.loc[pd.Series(cross_type, index=self.df.index).notnull()].copy()
        crosses['Cross_Type'] = cross_type[pd.Series(cross_type, index=self.df.index).notnull()]
        # return crosses[['datetime', 'open', 'high', 'low', 'close', 'volume', 'EMA_short', 'EMA_long', 'Cross_Type']]
        # print(f"Found {len(crosses)} crosses (Golden/Death) in data with {len(self.df)} rows.")
        # print(crosses.to_string())
        return crosses  # return all columns intact
    
    def get_all_trades(self) -> pd.DataFrame:
        crosses = self.find_crosses()
        trades = []
        position = None  # 'Long', 'Short', or None

        for idx, row in crosses.iterrows():
            if row.Cross_Type == 'Golden':
                # Close short trades if any
                if position == 'Short':
                    trades[-1]['Exit_Date'] = row['datetime']
                    trades[-1]['Exit_Price'] = row['close']
                    position = None
                # Open long trades if no position
                if position is None:
                    trades.append({
                        **row.to_dict(),  # retain all original columns
                        'Trade_Type': 'Long',
                        'Entry_Date': row['datetime'],
                        'Entry_Price': row['close'],
                        'Exit_Date': pd.NaT,
                        'Exit_Price': None,
                    })
                    position = 'Long'

            elif row.Cross_Type == 'Death':
                # Close long trades if any
                if position == 'Long':
                    trades[-1]['Exit_Date'] = row['datetime']
                    trades[-1]['Exit_Price'] = row['close']
                    position = None
                # Open short trades if no position
                if position is None:
                    trades.append({
                        **row.to_dict(),  # retain all original columns
                        'Trade_Type': 'Short',
                        'Entry_Date': row['datetime'],
                        'Entry_Price': row['close'],
                        'Exit_Date': pd.NaT,
                        'Exit_Price': None,
                    })
                    position = 'Short'
        trades_df = pd.DataFrame(trades)
        return trades_df


    def get_latest_trade(self) -> dict:
        trades_df = self.get_all_trades()
        if not trades_df.empty:
            return trades_df.iloc[-1].to_dict()
        return {}
    
    def update_crosses_in_df(self):
        crosses = self.find_crosses()

        # Initialize Cross_Type column with None
        self.df['Cross_Type'] = None
        self.df.loc[crosses.index, 'Cross_Type'] = crosses['Cross_Type']

        # add a new column named "Price_From_Cross" which is percentage of how much price has moved from last cross (positive or negative)
        # in "Price_From_Cross" column, from the row where first cross occured, price will be 0.00, in next row it will be the % change from that cross price, and so on.
        # when cross changes, say from Death to Golden, we need to start with the new cross price, again in the row with this cross, Price_From_Cross will be 0.00, and in next row it will be the % change from this new cross price.
        self.df['Price_From_Cross'] = 0.0
        last_cross_price = None
        for i in range(1, len(self.df)):
            if self.df.at[i, 'Cross_Type'] == 'Golden':
                last_cross_price = self.df.at[i, 'close']
            elif self.df.at[i, 'Cross_Type'] == 'Death':
                last_cross_price = self.df.at[i, 'close']
            if last_cross_price is not None:
                self.df.at[i, 'Price_From_Cross'] = ((self.df.at[i, 'close'] - last_cross_price) / last_cross_price * 100).round(2)
        
        # Forward fill Cross_Type - keep the cross type until next cross occurs
        self.df['Cross_Type'] = self.df['Cross_Type'].ffill()
        
        # Add Days_From_Cross column
        self.df['Days_From_Cross'] = None
        
        # Get today's date for calculation
        today = pd.Timestamp('today').normalize()
        
        # For each cross event, calculate days from that cross for subsequent rows
        for cross_idx in crosses.index:
            cross_date = self.df.loc[cross_idx, 'datetime']
            cross_type = crosses.loc[cross_idx, 'Cross_Type']
            
            # Find all rows from this cross until the next cross (or end of data)
            remaining_crosses = crosses.index[crosses.index > cross_idx]
            if len(remaining_crosses) > 0:
                next_cross_idx = remaining_crosses[0]
                affected_rows = self.df.index[(self.df.index >= cross_idx) & (self.df.index < next_cross_idx)]
            else:
                # This is the last cross, affect all remaining rows
                affected_rows = self.df.index[self.df.index >= cross_idx]
            
            # Calculate days from cross for each affected row
            for row_idx in affected_rows:
                row_date = self.df.loc[row_idx, 'datetime']
                days_diff = (row_date - cross_date).days
                self.df.loc[row_idx, 'Days_From_Cross'] = days_diff
        
        return self.df

# -----------------------------------------------------------------------------
def function_try_on_sample_file():
    #Working example to find ema crosses and calculate pnl

    file_name = "NSE_EQ|INE158A01026.parquet"
    df = read_instrument_data(input_folder, file_name)

    price = PriceResampler(df)
    monthly_df = price.to_weekly()
    analyzer = EMACrossAnalyzer(monthly_df, span_short=11, span_long=51)

    print_length = 100

    # print all the historic trades from available data
    all_trades = analyzer.get_all_trades()    
    print("_" * print_length)
    print("All Trades:")
    print(all_trades)

    output_folder_path = "/Users/piyush.potdukhe/Desktop/my_repos/restart/generated_outputs"
    output_file_path = os.path.join(output_folder_path, "df_to_check.csv")
    # df_to_check = analyzer.update_crosses_in_df()
    analyzer.df.to_csv(output_file_path, index=False)


    # get only the latest trade
    latest_trade = analyzer.get_latest_trade()
    print("_" * print_length)
    print("\nLatest Trade:")
    print(latest_trade)

    # filter all the trades that we need to calculate PnL for.
    all_trades_for_pnl = all_trades.iloc[:-1] # Exclude last trade as it is yet to close.
    # all_trades_for_pnl = all_trades_for_pnl[all_trades_for_pnl["Trade_Type"] == "Long"] # take only long trades.

    initial_capital = 100000
    calculator = PnLCalculator(all_trades_for_pnl, initial_capital) 
    result_df = calculator.calculate_pnl()
    print(result_df[['Trade_Type', 'Entry_Date', 'Entry_Price', 'Exit_Date', 'Exit_Price', 'PnL_Points', 'PnL_Amount', 'PnL_Percentage']])

    aggregated_pnl_stats = calculator.aggregate_pnl_stats()
    print(aggregated_pnl_stats.iloc[0])
    


# -----------------------------------------------------------------------------
def function_try_on_all_files_in_folder():
    df_all_latest_trades_rows = []

    files = [f for f in os.listdir(input_folder) if os.path.isfile(os.path.join(input_folder, f))]
    count = 0
    for filename in tqdm(files, desc="Processing files (EMA Analyser)"):
        file_path = os.path.join(input_folder, filename)
        df = read_instrument_data(input_folder, filename)
        price = PriceResampler(df)
        monthly_df = price.to_weekly()
        analyzer = EMACrossAnalyzer(monthly_df, span_short=11, span_long=51)
        latest_trade = analyzer.get_latest_trade()
        if latest_trade:
            df_all_latest_trades_rows.append(latest_trade)
        count += 1
        if 100 == count:
            break  # for testing, limit to first 100 files

    df_all_latest_trades = pd.DataFrame(df_all_latest_trades_rows)
    print(len(df_all_latest_trades))
    print(df_all_latest_trades.tail(10))
    
    # sort by days since entry
    today = pd.Timestamp('today').normalize()  # Current date without time
    df_all_latest_trades['Entry_Date'] = pd.to_datetime(df_all_latest_trades['Entry_Date'])
    df_all_latest_trades['Days_Since_Entry'] = (today - df_all_latest_trades['Entry_Date']).dt.days
    df_all_latest_trades = df_all_latest_trades.sort_values('Days_Since_Entry', ascending=True)
    print(df_all_latest_trades.head(11))
    print(df_all_latest_trades.tail(11))

    # save output to a file locally, FOR NOW ONLY.
    os.makedirs(output_folder, exist_ok=True)
    output_file_path = os.path.join(output_folder,  "all_latest_trades_ema_strategy.csv")
    df_all_latest_trades.to_csv(output_file_path, index=False)
    print(f"Saved latest trades to {output_file_path}")

# -----------------------------------------------------------------------------


if __name__ == "__main__":
    # function_try_on_all_files_in_folder()
    # function_try_on_sample_file()

    # now output_folder will have sub-folders daily, weekly, monthly which will have resampled data already
    # calculate and:
    #   add EMA_Long and EMA_Short for each row in each file in each folder and save output to a new column in same file.
    #   add EMA_cross info column as "span_short x span_long" for info in final output file.
    #   add Cross_Type column with values "Golden", "Death", or None
    #   add Days_From_Cross column with number of days since last cross (positive integer), or None if no cross yet.
    # save output to same file, overwriting it.

    freq_folders = {freq: os.path.join(output_folder, freq) for freq in output_sub_folder_list}
    for folder in freq_folders.values():
        files = [f for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f))]
        for filename in tqdm(files, desc=f"Processing files (EMA Analyser) in {os.path.relpath(folder)}"):
            file_path = os.path.join(folder, filename)
            df = read_instrument_data(folder, filename)
            if df.empty:
                continue
            analyzer = EMACrossAnalyzer(df, span_short=11, span_long=51)
            out_path = os.path.join(folder, filename)  # overwrite same file
            analyzer.df.to_parquet(out_path, index=False)
    