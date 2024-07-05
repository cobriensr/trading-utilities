"""Standardized Trading Analysis for LuxAlgo® Backtesting System™ (S&O)"""

import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text

# pylint: disable=missing-function-docstring, missing-class-docstring, trailing-whitespace, line-too-long, missing-final-newline, logging-fstring-interpolation, broad-exception-caught, redefined-outer-name

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# extract password from environment variable
password = os.environ.get("POSTGRES_PASSWORD")
username = os.environ.get("USERNAME")

# Constants
INITIAL_CAPITAL = 20000
DB_URL = "postgresql://username:password@localhost:5432/trading_db"

def load_data_1() -> pd.DataFrame:
    """Load and preprocess data from the database."""
    try:
        engine = create_engine(DB_URL)
        query = text("SELECT * FROM trades ORDER BY date DESC, time DESC")
        df = pd.read_sql(query, engine)

        df["date"] = pd.to_datetime(df["date"])

        # Calculate the equity
        df['equity'] = INITIAL_CAPITAL + df['profit'].cumsum()

        logger.info("Data loaded and preprocessed successfully")
        return df
    except Exception as e:
        logger.exception(f"Error loading data: {e}")
        return pd.DataFrame()

df_1 = load_data_1()

def create_pivot_tables_1(df_1) -> pd.DataFrame:
    pivot_1 = pd.pivot_table(df_1, values=['profit_usd'], index=['date'], columns=['hour'], aggfunc='sum', fill_value=0)
    pivot_2 = pd.pivot_table(df_1, values=['profit_usd'], index=['weekday'], columns=['hour'], aggfunc='sum', fill_value=0)
    pivot_3 = pd.pivot_table(df_1, values=['profit_usd'], index=['weeknum'], columns=['hour'], aggfunc='sum', fill_value=0)
    pivot_4 = pd.pivot_table(df_1, values=['profit_usd'], index=['type'], columns=['hour'], aggfunc='sum', fill_value=0)
    return pivot_1, pivot_2, pivot_3, pivot_4

def load_data_2() -> pd.DataFrame:
    """Load and preprocess data from the database."""
    try:
        engine = create_engine(DB_URL)
        query = text("SELECT * FROM trades_2 ORDER BY date DESC, time DESC")
        df = pd.read_sql(query, engine)

        df["date"] = pd.to_datetime(df["date"])

        # Calculate the equity
        df['equity'] = INITIAL_CAPITAL + df['profit'].cumsum()

        logger.info("Data loaded and preprocessed successfully")
        return df
    except Exception as e:
        logger.exception(f"Error loading data: {e}")
        return pd.DataFrame()

df_2 = load_data_2()

def create_pivot_tables_2(df_2) -> pd.DataFrame:
    pivot_1 = pd.pivot_table(df_2, values=['profit_usd'], index=['date'], columns=['hour'], aggfunc='sum', fill_value=0)
    pivot_2 = pd.pivot_table(df_2, values=['profit_usd'], index=['weekday'], columns=['hour'], aggfunc='sum', fill_value=0)
    pivot_3 = pd.pivot_table(df_2, values=['profit_usd'], index=['weeknum'], columns=['hour'], aggfunc='sum', fill_value=0)
    pivot_4 = pd.pivot_table(df_2, values=['profit_usd'], index=['type'], columns=['hour'], aggfunc='sum', fill_value=0)
    return pivot_1, pivot_2, pivot_3, pivot_4