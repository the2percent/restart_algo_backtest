# Financial Data Analysis & Trading Strategy Framework

A comprehensive Python framework for analyzing financial market data, implementing technical indicators, and evaluating trading strategies with automated backtesting capabilities.

## 🚀 Features

- **Data Processing**: Automated data fetching and resampling (daily, weekly, monthly)
- **Technical Analysis**: EMA crosses, RSI indicators, volume analysis
- **Trading Strategies**: Golden/Death cross strategies with P&L calculation
- **Backtesting**: Comprehensive backtesting with performance metrics
- **Automation**: One-command execution of entire analysis pipeline

## 📋 Project Structure

```
├── all_constants/           # Configuration and constants
├── data_analyser_algos/     # Technical analysis algorithms
│   ├── ema_analyser.py      # EMA cross analysis
│   ├── rsi_analyser.py      # RSI indicator analysis
│   ├── volume_analyser.py   # Volume analysis
│   └── misc_analyser.py     # Additional analysis tools
├── data_fetcher/            # Data acquisition modules
├── data_pnl/               # P&L calculation engine
├── data_resampler/         # Data resampling utilities
├── run_all_analyzers.py    # Main execution script
└── README.md
```

## 🛠️ Installation

1. **Clone the repository**:
   ```bash
   git clone <your-repo-url>
   cd restart
   ```

2. **Create and activate virtual environment**:
   ```bash
   python -m venv myenv
   source myenv/bin/activate  # On Windows: myenv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install pandas numpy tqdm pyarrow
   ```

## 🚀 Quick Start

### Run Complete Analysis Pipeline
```bash
python run_all_analyzers.py
```

This will execute all modules in sequence:
1. Data fetching and preparation
2. EMA analysis
3. RSI analysis  
4. Volume analysis
5. Results filtering and summary

### Run Individual Modules
```bash
# EMA Cross Analysis
python -m data_analyser_algos.ema_analyser

# RSI Analysis
python -m data_analyser_algos.rsi_analyser

# Volume Analysis
python -m data_analyser_algos.volume_analyser
```

## 📊 Key Components

### EMA Cross Analyzer
- Calculates short and long-term EMAs
- Identifies Golden Cross (bullish) and Death Cross (bearish) signals
- Tracks days since last cross
- Calculates price movement percentage from cross points

### RSI Analyzer
- Computes Relative Strength Index
- Identifies overbought/oversold conditions
- Tracks RSI trends and divergences

### Volume Analyzer
- Calculates volume percentiles
- Computes volume EMAs (20, 50, 200 periods)
- Identifies unusual volume activity

### P&L Calculator
- Backtests trading strategies
- Calculates trade-by-trade P&L
- Provides comprehensive performance metrics
- Computes CAGR, drawdowns, and win rates

## 📈 Analysis Features

### Cross Analysis
- **Cross Type**: Tracks current market state (Golden/Death)
- **Days From Cross**: Days elapsed since last cross event
- **Price From Cross**: Percentage price movement from cross point

### Performance Metrics
- Absolute returns (points, amount, percentage)
- Annualized returns and CAGR
- Maximum drawdown analysis
- Trade statistics and holding periods

## 🔧 Configuration

Update constants in `all_constants/constants.py`:
- Input/output folder paths
- EMA periods (default: 11, 51)
- RSI parameters
- Analysis timeframes

## 📊 Output Files

The framework generates:
- **Resampled Data**: Daily, weekly, monthly OHLC data
- **Analysis Results**: Technical indicators and signals
- **Trading Reports**: Backtesting results and P&L analysis
- **Summary Files**: Latest trades and portfolio performance

## 🧪 Example Usage

```python
from data_analyser_algos.ema_analyser import EMACrossAnalyzer
from data_resampler.price_resampler import PriceResampler

# Load and analyze data
df = read_instrument_data(folder_path, filename)
resampler = PriceResampler(df)
weekly_data = resampler.to_weekly()

# Analyze EMA crosses
analyzer = EMACrossAnalyzer(weekly_data, span_short=11, span_long=51)
latest_trade = analyzer.get_latest_trade()
all_trades = analyzer.get_all_trades()
```

## 📋 Requirements

- Python 3.8+
- pandas
- numpy
- tqdm
- pyarrow (for parquet files)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## ⚠️ Disclaimer

This software is for educational and research purposes only. It is not financial advice. Trading involves risk and you should consult with a qualified financial advisor before making any investment decisions.

## 📞 Support

For questions and support, please open an issue in the GitHub repository.

---

**Happy Trading! 📈**