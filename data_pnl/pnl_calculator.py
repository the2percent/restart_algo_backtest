import pandas as pd
import numpy as np

class PnLCalculator:
    def __init__(self, df, capital):
        self.df = df.copy()
        self.capital = capital
        self.decimal_round_value = 2
        self._validate_columns()

    def _validate_columns(self):
        required_columns = {'Trade_Type', 'Entry_Date', 'Entry_Price', 'Exit_Date', 'Exit_Price'}
        if not required_columns.issubset(self.df.columns):
            missing = required_columns - set(self.df.columns)
            raise ValueError(f"Missing required columns: {missing}")

    def calculate_pnl(self):
        print("\n" + "-" * 50 + "<< Calculate PnL >>" + "-" * 50)

        def pnl_points(row):
            if row['Trade_Type'] == 'Long':
                return row['Exit_Price'] - row['Entry_Price']
            elif row['Trade_Type'] == 'Short':
                return row['Entry_Price'] - row['Exit_Price']
            else:
                raise ValueError(f"Invalid Trade_Type: {row['Trade_Type']}")
        
        def pnl_amount(row):
            entry_price = row['Entry_Price']
            traded_quantity = self.capital / entry_price
            trade_pnl_amount = row['PnL_Points'] * traded_quantity
            return round(trade_pnl_amount, self.decimal_round_value)

        self.df['PnL_Points'] = self.df.apply(pnl_points, axis=1)
        self.df['PnL_Amount'] = self.df.apply(pnl_amount, axis=1)
        self.df['PnL_Percentage'] = round((self.df['PnL_Amount'] / self.capital) * 100, self.decimal_round_value)
        return self.df

    
    def aggregate_pnl_stats(self):
        print("\n" + "-" * 50 + "<< Aggregated PnL Stats >>" + "-" * 50)

        def days_to_ymd(mdays: int) -> str:
            mdays = int(round(mdays))
            years = mdays // 365
            remaining_days = mdays % 365
            months = remaining_days // 30
            days = remaining_days % 30
            return f"{years}.{months}.{days}"

        # Ensure Entry_Date and Exit_Date are datetime
        self.df['Entry_Date'] = pd.to_datetime(self.df['Entry_Date'])
        self.df['Exit_Date'] = pd.to_datetime(self.df['Exit_Date'])

        # Number of trades
        total_trades = len(self.df)

        # Absolute return
        absolute_return_points = self.df['PnL_Points'].sum()
        absolute_return_amount = self.df['PnL_Amount'].sum()
        total_traded_days = (self.df['Exit_Date'].max() - self.df['Entry_Date'].min()).days
        total_traded_years = total_traded_days // 365
        average_holding_period_days = self.df['Exit_Date'].subtract(self.df['Entry_Date']).dt.days.mean()
        highest_holding_period_days = self.df['Exit_Date'].subtract(self.df['Entry_Date']).dt.days.max()

        # Return percentages
        absolute_returns_percentage = round((absolute_return_amount / self.capital) * 100, self.decimal_round_value)

        if total_traded_days >= 365:
            yearly_returns_percentage = round(absolute_returns_percentage / (total_traded_days / 365), self.decimal_round_value)
        else:
            yearly_returns_percentage = np.nan

        # CAGR returns percentage
        end_value = self.capital + absolute_return_amount
        if total_traded_years > 0:
            cagr = ((end_value / self.capital) ** (1 / total_traded_years)) - 1
            CAGR_returns_percentage = round(cagr * 100, self.decimal_round_value)
        else:
            CAGR_returns_percentage = np.nan


        # Calculate drawdown
        # Sort by Exit_Date and get cumulative PnL
        self.df = self.df.sort_values(by='Exit_Date')
        self.df['cum_pnl'] = self.df['PnL_Amount'].cumsum()
        peak = self.df['cum_pnl'].cummax()
        drawdown = peak - self.df['cum_pnl']
        max_drawdown_amount = round(drawdown.max(), self.decimal_round_value)

        # Drawdown percentage relative to peak
        with np.errstate(divide='ignore', invalid='ignore'):
            drawdown_percents = np.where(peak == 0, 0, drawdown / peak * 100)
        max_drawdown_percentage = round(np.max(drawdown_percents), self.decimal_round_value)

        # Compile stats into a returnable DataFrame
        stats = {
            'total_trades': total_trades,
            'traded_period': days_to_ymd(total_traded_days),
            'highest_holding_period' : days_to_ymd(highest_holding_period_days),
            "average_holding_period": days_to_ymd(average_holding_period_days),
            'absolute_return_points': absolute_return_points,
            'absolute_return_amount': absolute_return_amount,
            'absolute_returns_percentage': absolute_returns_percentage,
            'yearly_returns_percentage': yearly_returns_percentage,
            'CAGR_returns_percentage': str(CAGR_returns_percentage) + " (calcs to be recheck)",
            'max_drawdown_amount': str(max_drawdown_amount) + " (calcs to be recheck)",
            'max_drawdown_percentage': str(max_drawdown_percentage) + " (calcs to be recheck)"
        }

        return pd.DataFrame([stats])


# Usage:
# df = pd.DataFrame(...)  # Your trades data
# calculator = PnLCalculator(df)
# result_df = calculator.calculate_pnl()
# print(result_df)
