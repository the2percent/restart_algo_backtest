
import pandas as pd

class PriceResampler:
    """Handles conversion of daily OHLC data into weekly and monthly aggregates using efficient resampling."""
    def __init__(self, df: pd.DataFrame):
        self.df = df.sort_values('datetime').copy()
    
    def to_daily(self) -> pd.DataFrame:
        # downloaded data is already in daily format, just add the resampled_price_type column
        out = self.df.copy()
        out['resampled_price_type'] = 'Daily'
        out = out[['instrument_name', 'instrument_key', 'resampled_price_type', 'datetime', 'open', 'high', 'low', 'close', 'volume', 'open_interest']]
        return out
    
    def to_weekly(self) -> pd.DataFrame:
        # Resample calendar weeks from Monday, aggregate
        out = self.df.resample(
            'W-MON',            # Weekly, weeks starting on Monday
            on='datetime',
            label='left',
            closed='left'
        ).agg({
            'instrument_name': 'first',
            'instrument_key': 'first',
            'open_interest': 'first',
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        })

        # For each weekly bucket, set the label to first trading date in that week
        # else above code sets it to calendar week start which may not be trading day
        weeks = out.index
        corrected_dates = []

        for wdate in weeks:
            # Get actual first date within that week
            week_df = self.df[(self.df['datetime'] >= wdate) & (self.df['datetime'] < wdate + pd.Timedelta(days=7))]
            if not week_df.empty:
                corrected_dates.append(week_df.iloc[0]['datetime'])
            else:
                # Fallback to calendar date if no data
                corrected_dates.append(wdate)

        out = out.reset_index(drop=True)
        out['datetime'] = corrected_dates
        out['resampled_price_type'] = 'Weekly'
        out = out[['instrument_name', 'instrument_key', 'resampled_price_type', 'datetime', 'open', 'high', 'low', 'close', 'volume', 'open_interest']]
        return out

    def to_monthly(self) -> pd.DataFrame:
    # Resample calendar months from the first, aggregate
        out = self.df.resample(
            'MS',        
            on='datetime',
            label='left',
            closed='left'
        ).agg({
            'instrument_name': 'first',
            'instrument_key': 'first',
            'open_interest': 'first',
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        })

        # For each monthly bucket, set the label to first trading date in that month
        # else above code sets it to calendar month start which may not be trading day
        months = out.index
        corrected_dates = []

        for mdate in months:
            # Get the actual first date within that bucket in source df
            month_df = self.df[(self.df['datetime'] >= mdate) & (self.df['datetime'] < mdate + pd.offsets.MonthBegin(1))]
            if not month_df.empty:
                corrected_dates.append(month_df.iloc[0]['datetime'])
            else:
                # Fallback to calendar date, if no data
                corrected_dates.append(mdate)
        
        out = out.reset_index(drop=True)
        out['datetime'] = corrected_dates
        out['resampled_price_type'] = 'Monthly'
        out = out[['instrument_name', 'instrument_key', 'resampled_price_type', 'datetime', 'open', 'high', 'low', 'close', 'volume', 'open_interest']]
        return out