import os
import glob
import pandas as pd
from datetime import datetime
import yfinance as yf
import talib as ta

# Set your folder path
folder_path = '../credential/portfolio/equity/'

# Fetch all matching files
files = glob.glob(os.path.join(folder_path, 'equity_portfolio_*.csv'))
file_dates = []
for file in files:
    try:
        date_str = os.path.basename(file).split('_')[-1].replace('.csv', '')
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        file_dates.append((file, date_obj))
    except Exception as e:
        print(f"Skipping {file}: {e}")

if not file_dates:
    raise ValueError("No valid files found.")

latest_file = max(file_dates, key=lambda x: x[1])[0]

print(f"Reading latest file: {latest_file}")

# Read the latest file
df = pd.read_csv(latest_file)
symbols = df['tradingsymbol'].dropna().unique().tolist()
symbols = [i+".NS" for i in symbols]
print(f"Total symbols found: {len(symbols)}")
print(symbols)
output_path = os.path.join(folder_path, 'symbols_list.csv')
symbols_df = pd.DataFrame(symbols, columns=['Symbol'])
symbols_df.to_csv(output_path, index=False)



# Function to fetch data using yfinance
def fetch_data(symbol):
    stock = yf.Ticker(symbol)
    info = stock.info
    try:
        # Extract necessary fields
        pe_ratio = info.get('trailingPE', None)
        pb_ratio = info.get('priceToBook', None)
        revenue_growth = info.get('revenueGrowth', None)
        market_price = info.get('currentPrice', None)
        earnings = info.get('earningsQuarterlyGrowth', None)

        # Download historical stock data for technical indicators
        hist = stock.history(period="1y")  # 1 year of data

        # Calculate RSI and MACD
        rsi = ta.RSI(hist['Close'], timeperiod=14)[-1]  # 14-day RSI
        macd, macd_signal, macd_hist = ta.MACD(hist['Close'], fastperiod=12, slowperiod=26, signalperiod=9)

        return {
            'Symbol': symbol,
            'P/E': pe_ratio,
            'P/B': pb_ratio,
            'Revenue Growth': revenue_growth,
            'Price': market_price,
            'Earnings Growth': earnings,
            'RSI': rsi,
            'MACD': macd[-1],  # Latest MACD value
            'MACD Signal': macd_signal[-1],  # Latest MACD Signal value
            'MACD Hist': macd_hist[-1]  # Latest MACD Histogram value
        }
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return None

# Define threshold values for flagging overvalued stocks
PE_THRESHOLD = 30  # Arbitrary high P/E ratio threshold
PRICE_REVENUE_GROWTH_THRESHOLD = 1.5  # If price growth is higher than 1.5x revenue growth, flag
RSI_OVERBOUGHT_THRESHOLD = 70  # RSI > 70 indicates overbought (potentially overvalued)
MACD_OVERBOUGHT_THRESHOLD = 0  # MACD above 0 may indicate overbought condition

# Collect data for all stocks in your portfolio
stock_data = []
for symbol in symbols:
    data = fetch_data(symbol)
    if data:
        stock_data.append(data)

# Create a DataFrame from the collected data
df = pd.DataFrame(stock_data)

# Apply conditions to flag overvalued stocks
df['Overvalued'] = ((df['P/E'] > PE_THRESHOLD) | 
                    (df['Price'] / df['Revenue Growth'] > PRICE_REVENUE_GROWTH_THRESHOLD) | 
                    (df['RSI'] > RSI_OVERBOUGHT_THRESHOLD) |
                    (df['MACD'] > MACD_OVERBOUGHT_THRESHOLD)).astype(int)
current_date = datetime.now().strftime("%Y-%m-%d")

# Save the result to a CSV
output_path = f'output/overvalued_stocks_with_technical_indicators_{current_date}.csv'
df.to_csv(output_path, index=False)

print(f"Overvalued stocks flagged and saved to {output_path}")
print(df[['Symbol', 'P/E', 'Revenue Growth', 'Price', 'RSI', 'MACD', 'MACD Signal', 'Overvalued']])
