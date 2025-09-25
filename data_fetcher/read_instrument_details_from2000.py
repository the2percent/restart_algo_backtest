from datetime import datetime
from dateutil.relativedelta import relativedelta
import logging
from tqdm import tqdm
import ijson
import os
import pandas as pd
import requests
import gzip
from io import TextIOWrapper


# Setup logging
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Function to read and filter instrument details from a remote gzipped JSON file
def get_master_instrument_details():
    url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
    
    filtered_nodes = []
    
    with requests.get(url, stream=True) as r:
        r.raise_for_status()

        # This uses no local disk storage and efficiently handles large compressed JSON files
        # directly from the remote URL while streaming and filtering on-the-fly. It is optimal
        # for cloud or ephemeral environments without persistent file systems.
        gz = gzip.GzipFile(fileobj=r.raw)                   # Wrap raw socket stream with gzip decompress
        text_stream = TextIOWrapper(gz, encoding='utf-8')   # Wrap decompressed binary stream into text stream for ijson

        with tqdm(desc="Preparing Instrument List") as pbar:
            for node in ijson.items(text_stream, 'item'):
                pbar.update(1)
                if (node.get('segment') == 'NSE_EQ' and 
                    node.get('security_type') == 'NORMAL' and 
                    node.get('instrument_type') == 'EQ'):
                    filtered_nodes.append(node)
    df = pd.DataFrame(filtered_nodes)
    logger.info(f"Total filtered nodes after all criteria / filtered-stock-list = {len(df)}")
    return df

def getDataFromUpstox(start_date, end_date, instrument_key, instrument_name, interval="day"):
    # upstox provides last 10 years of data.
    url = f"https://api-v2.upstox.com/historical-candle/{instrument_key}/{interval}/{end_date}/{start_date}" # Note: /to/from
    headers = {
        "Api-Version": "2.0",
        "Accept": "application/json",
        "Cookie": "test"
    }
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Error: {response.status_code} - {response.text}")
    data = response.json()
    candles = data.get("data", {}).get("candles", [])
    df = pd.DataFrame(candles, columns=["datetime", "open", "high", "low", "close", "volume", "open_interest"])
    df["datetime"] = pd.to_datetime(df["datetime"]).dt.tz_localize(None)

    df[["open", "high", "low", "close", "volume", "open_interest"]] = df[["open", "high", "low", "close", "volume", "open_interest"]].astype(float)
    if not df.empty:
        # logger.info(f"Data download successfully for {instrument_name} :: {instrument_key}, rows: {len(df)}")
        df.insert(1, "instrument_name", instrument_name)
        df.insert(2, "instrument_key", instrument_key)

    return df

def save_data_locally(df, ticker, folder="downloaded_instrument_data"):
    os.makedirs(folder, exist_ok=True) # create folder if not exists, in the path where this is run
    file_path = os.path.join(folder, f"{ticker}.parquet")
    # Save as compressed parquet for disk efficiency and fast reload
    df.to_parquet(file_path, compression='snappy')
    # logger.info(f"Downloaded && Saved data for {ticker} at {file_path}")

def generate_10_year_ranges(start_date, end_date):
    ranges = []
    current_start = start_date
    while current_start <= end_date:
        current_end = min(current_start + relativedelta(years=10) - relativedelta(days=1), end_date)
        ranges.append((current_start.strftime('%Y-%m-%d'), current_end.strftime('%Y-%m-%d')))
        current_start = current_end + relativedelta(days=1)
    return ranges

def download_and_store_all_auto_date(instruments_df, interval="day"):
    overall_start_date = datetime(2000, 1, 1)
    overall_end_date = datetime.today()
    
    ranges = generate_10_year_ranges(overall_start_date, overall_end_date)
    
    total_tasks = len(instruments_df)# * len(ranges)
    with tqdm(total=total_tasks, desc="Downloading Instruments (from yr 2000)") as pbar:
        for row in instruments_df.itertuples(index=False):
            instrument_key = row.instrument_key
            instrument_name = row.name
            dfs = []
            for start_date, end_date in ranges:
                try:
                    df_block = getDataFromUpstox(start_date, end_date, instrument_key, instrument_name, interval)
                    if not df_block.empty:
                        dfs.append(df_block)
                except Exception as e:
                    logging.error(f"Error downloading {instrument_name} ({start_date} to {end_date}): {e}")
                finally:
                    pbar.update(1)
            if dfs:
                full_df = pd.concat(dfs, ignore_index=True)
                save_data_locally(full_df, instrument_key)

if __name__ == "__main__":
    instrument_details = get_master_instrument_details()
    # instrument_details = instrument_details.tail(5) # For testing, limit to last 5 entries
    download_and_store_all_auto_date(instrument_details)
    
    # read_data = pd.read_parquet("downloaded_instrument_data/NSE_EQ|INF109KC1V59.parquet")
    # print(read_data)

    # # sort data on datetime column 
    # read_data = read_data.sort_values(by="datetime", ascending=False).reset_index(drop=True)
    # print("after sorting")
    # print(read_data)

    # read_data = pd.read_parquet("downloaded_instrument_data/NSE_EQ|INE0CLI01024.parquet")
    # print(read_data)

    # print(read_data.tail(5))
    # print(read_data.head(5))
