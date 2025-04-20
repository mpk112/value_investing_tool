import argparse
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os,lxml
from nsetools import Nse
import requests
from bs4 import BeautifulSoup
from nsepy import get_history



def fetch_etf_list():
    df = pd.read_csv("etf/etf_list_nse.csv")
    etf_list = df["SYMBOL"].unique().tolist()
    return etf_list
def fetch_nifty500_list():
    df = pd.read_csv("nifty/nifty500.csv")
    etf_list = df["SYMBOL"].unique().tolist()
    return etf_list

def fetch_data(tickers, start_date, end_date):
    os.makedirs('historical_data', exist_ok=True)

    # Ensure start_date and end_date are strings in the format '%Y-%m-%d'
    if isinstance(start_date, datetime):
        start_date = start_date.strftime('%Y-%m-%d')
    if isinstance(end_date, datetime):
        end_date = end_date.strftime('%Y-%m-%d')

    for ticker in tickers:
        try:
            # Append '.NS' to ticker symbol for NSE stocks
            yf_ticker = f"{ticker}.NS"

            # Fetch historical data
            data = yf.download(yf_ticker, start=start_date, end=end_date)

            if data.empty:
                print(f'No data found for {ticker} between {start_date} and {end_date}.')
                continue

            data.reset_index(inplace=True)
            data['Ticker'] = ticker
            start_str = datetime.strptime(start_date, '%Y-%m-%d').strftime('%Y%m%d')
            end_str = datetime.strptime(end_date, '%Y-%m-%d').strftime('%Y%m%d')
            filename = f'historical_data_yfin/{ticker}_{start_str}_{end_str}_historical_data.csv'
            data.to_csv(filename, index=False)
            print(f'Data for {ticker} saved successfully to {filename}.')
        except Exception as e:
            print(f'Error fetching data for {ticker}: {e}')
            
def fetch_data_nse(tickers, start_date, end_date):
    os.makedirs('historical_data_yfin', exist_ok=True)

    # Ensure start_date and end_date are strings in the format '%Y-%m-%d'
    if isinstance(start_date, datetime):
        start_date = start_date.strftime('%Y-%m-%d')
    if isinstance(end_date, datetime):
        end_date = end_date.strftime('%Y-%m-%d')

    # Convert string dates to datetime.date objects
    start = datetime.strptime(start_date, '%Y-%m-%d').date()
    end = datetime.strptime(end_date, '%Y-%m-%d').date()

    for ticker in tickers:
        try:
            # Fetch historical data
            data = get_history(symbol=ticker, start=start, end=end,index=
                               True)

            if data.empty:
                print(f'No data found for {ticker} between {start_date} and {end_date}.')
                continue

            start_str = start.strftime('%Y%m%d')
            end_str = end.strftime('%Y%m%d')
            data.reset_index(inplace=True)
            data['Ticker'] = ticker
            data.to_csv(f'historical_data_yfin/{ticker}_{start_str}_{end_str}_historical_data.csv', index=False)
            print(f'Data for {ticker} saved successfully.')
        except Exception as e:
            print(f'Error fetching data for {ticker}: {e}')
    
def main():
    
    nifty_500 = fetch_nifty500_list()
    etf_list = fetch_etf_list()
    
    parser = argparse.ArgumentParser(description="Fetch historical stock and ETF data.")
    
    # Define mutually exclusive group
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--all_tickers', action='store_true', help="Fetch data for all tickers.")
    group.add_argument('--select_tickers', type=str, nargs='+', help="Fetch data for selected tickers.")

    # Add date arguments
    parser.add_argument('--start_date', type=str, default=(datetime.today() - timedelta(days=500)).strftime('%Y-%m-%d'),
                        help="Start date in YYYY-MM-DD format.")
    parser.add_argument('--end_date', type=str, default=datetime.today().strftime('%Y-%m-%d'),
                        help="End date in YYYY-MM-DD format.")

    args = parser.parse_args()

    # Determine tickers to fetch
    if args.all_tickers:
        # Replace with actual logic to fetch all tickers
        tickers = nifty_500+etf_list  
    elif args.select_tickers:
        tickers = args.select_tickers

    # Convert string dates to datetime objects
    start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d')

    # Fetch and save data
    fetch_data(tickers, start_date, end_date)

if __name__ == '__main__':
    main()
