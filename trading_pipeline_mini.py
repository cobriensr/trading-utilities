"""Standardized Trading Pipeline for LuxAlgo® Backtesting System™ (S&O)"""

from datetime import datetime, time, timedelta
import glob
import os
from typing import Optional
import pytz
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, Time, Enum
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import IntegrityError, SQLAlchemyError, OperationalError
from sqlalchemy.engine.url import URL
from sqlalchemy.orm.session import Session as SQLAlchemySession

# pylint: disable=missing-function-docstring, missing-class-docstring, trailing-whitespace, line-too-long, missing-final-newline

# Load the environment variables from .env file
load_dotenv()

# Database setup
Base = declarative_base()


class Trade(Base):
    __tablename__ = "trades_2"

    id = Column(Integer, primary_key=True)
    symbol = Column(String(10), nullable=False)
    type = Column(String(20), nullable=False)
    date = Column(Date, nullable=False)
    time = Column(Time, nullable=False)
    day = Column(Integer, nullable=False)
    hour = Column(Integer, nullable=False)
    minute = Column(Integer, nullable=False)
    weekday = Column(String(10), nullable=False)
    weeknum = Column(Integer, nullable=False)
    month = Column(String(10), nullable=False)
    year = Column(Integer, nullable=False)
    contracts = Column(Integer, nullable=False)
    margin = Column(Float, nullable=False)
    commission = Column(Float, nullable=False)
    profit_usd = Column(Float, nullable=False)
    win_loss = Column(Enum("Win", "Loss", name="win_loss"), nullable=False)
    strategy = Column(String(50), nullable=True)
    market_session = Column(
        Enum("Pre-Market", "Market Hours", "Post-Market", name="market_session"),
        nullable=False,
    )

    def __repr__(self):
        return f"<Trade(symbol='{self.symbol}', date='{self.date}', profit_usd='{self.profit_usd}')>"


# extract password from environment variable
password = os.getenv("POSTGRES_PASSWORD")
username = os.getenv("USERNAME")

# Check if env values are set
if password is None:
    raise ValueError("POSTGRES_PASSWORD environment variable is not set")
if username is None:
    raise ValueError("USERNAME environment variable is not set")

# create database URL connection string
db_url = URL.create(
    drivername="postgresql",
    username="username",
    password=password,
    host="localhost",
    port=5432,
    database="trading_db",
)

print(f"Connecting to: {db_url.render_as_string(hide_password=True)}")

try:
    engine = create_engine(db_url)
    connection = engine.connect()
    print("Connection successful!")
except OperationalError as oe:
    print(f"OperationalError: {str(oe)}")
    print(
        "This error often occurs due to network issues, database server being down, or incorrect connection details."
    )
except SQLAlchemyError as se:
    print(f"SQLAlchemyError: {str(se)}")
    print(
        "This is a general SQLAlchemy error. It could be due to configuration issues or other SQLAlchemy-specific problems."
    )

# Database schema
Base.metadata.create_all(engine)

# Database connection
Session = sessionmaker(bind=engine)


def find_file_in_date_range(
    directory: str,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> str:
    if start_date is None:
        start_date = datetime.now().date() - timedelta(days=7)  # Default to 7 days ago
    if end_date is None:
        end_date = datetime.now().date()

    all_files = []
    current_date = end_date
    while current_date >= start_date:
        date_str = current_date.strftime("%Y-%m-%d")
        pattern = f"{directory}/LuxAlgo®_-_Backtesting_System™_(S&O)_List_of_Trades_{date_str}_*.csv"
        files = glob.glob(pattern)
        all_files.extend(files)
        current_date -= timedelta(days=1)

    if not all_files:
        raise FileNotFoundError(
            f"No files found matching the pattern between {start_date} and {end_date}"
        )

    return max(all_files, key=os.path.getctime)


def find_latest_file(directory: str) -> str:
    today = datetime.now().strftime("%Y-%m-%d")
    pattern = (
        f"{directory}/LuxAlgo®_-_Backtesting_System™_(S&O)_List_of_Trades_{today}_*.csv"
    )
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(
            f"No file found matching the pattern for today's date: {today}"
        )
    return max(files, key=os.path.getctime)


def load_csv_to_dataframe(file_path: str) -> pd.DataFrame:
    return pd.read_csv(file_path)


def clean_and_transform_data(df: pd.DataFrame) -> pd.DataFrame:
    # Trade #
    # Type
    # Signal
    # Date/Time
    # Price USD
    # Contracts
    # Profit USD
    # Profit %
    # Cum. Profit USD
    # Cum. Profit %
    # Run-up USD
    # Run-up %
    # Drawdown USD
    # Drawdown %

    # Convert Date/Time column to datetime
    df["Date/Time"] = pd.to_datetime(df["Date/Time"], format="mixed")

    # Create new time series analysis columns
    df["date"] = df["Date/Time"].dt.date
    df["day"] = df["Date/Time"].dt.day
    df["time"] = df["Date/Time"].dt.time
    df["hour"] = df["Date/Time"].dt.hour
    df["minute"] = df["Date/Time"].dt.minute
    df["weekday"] = df["Date/Time"].dt.day_name()
    df["weeknum"] = df["Date/Time"].dt.isocalendar().week
    df["month"] = df["Date/Time"].dt.month_name()
    df["year"] = df["Date/Time"].dt.year

    # Add win/loss column
    df["win_loss"] = df["Profit USD"].apply(lambda x: "Win" if x > 0 else "Loss")

    # add strategy column
    df["Strategy"] = "1M Neocloud Micro"

    # add margin and commission columns
    df["Margin"] = (df["Contracts"] * 100.00).round(2)
    df["Commissions"] = (df["Contracts"] * 0.87).round(2)

    # add symbol column
    df["Symbol"] = "CL"

    # drop unneeded columns
    df = df.drop("Date/Time", axis=1)
    df = df.drop("Trade #", axis=1)
    df = df.drop("Price USD", axis=1)
    df = df.drop("Signal", axis=1)
    df = df.drop("Profit %", axis=1)
    df = df.drop("Cum. Profit USD", axis=1)
    df = df.drop("Cum. Profit %", axis=1)
    df = df.drop("Run-up USD", axis=1)
    df = df.drop("Run-up %", axis=1)
    df = df.drop("Drawdown USD", axis=1)
    df = df.drop("Drawdown %", axis=1)

    # record original length of dataframe
    original_length = len(df)

    # drop exit trades
    df = df[(df["Type"] != "Exit Short") & (df["Type"] != "Exit Long")]

    # record new length of dataframe
    new_length = len(df)

    # calculate difference
    rows_removed = original_length - new_length

    # print to the terminal the number of rows removed
    print(f"Removed {rows_removed} rows")

    # reset index of dataframe
    df = df.reset_index(drop=True)

    def get_market_session(date: datetime, hour: float, minute: float) -> str:
        # declare timezone
        central_tz = pytz.timezone("US/Central")
        # Convert hour and minute to integers
        hour_int = int(hour)
        minute_int = int(minute)

        # Convert hour and minute to datetime.time object
        trade_datetime = central_tz.localize(
            datetime.combine(date, time(hour_int, minute_int))
        )
        # Define market open and close times
        market_open = central_tz.localize(datetime.combine(date, time(8, 30)))
        market_close = central_tz.localize(datetime.combine(date, time(15, 0)))
        # Determine market session
        if market_open <= trade_datetime < market_close:
            return "Market Hours"
        elif trade_datetime < market_open:
            return "Pre-Market"
        else:
            return "Post-Market"

    # Apply market session lambda function
    df["Market Session"] = df.apply(
        lambda row: get_market_session(row["date"], row["hour"], row["minute"]), axis=1
    )

    # rename dataframe columns to match database columns
    column_mapping = {
        "Symbol": "symbol",
        "Type": "type",
        "Date": "date",
        "Time": "time",
        "Day": "day",
        "Hour": "hour",
        "Minute": "minute",
        "Weekday": "weekday",
        "Weeknum": "weeknum",
        "Month": "month",
        "Year": "year",
        "Contracts": "contracts",
        "Margin": "margin",
        "Commissions": "commission",
        "Profit USD": "profit_usd",
        "Win/Loss": "win_loss",
        "Strategy": "strategy",
        "Market Session": "market_session",
    }
    # Rename columns
    df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})

    # Remove rows with 'nan' values
    df = df.dropna()

    # Select only the columns we want in our database
    columns_to_keep = [
        "symbol",
        "type",
        "date",
        "time",
        "day",
        "hour",
        "minute",
        "weekday",
        "weeknum",
        "month",
        "year",
        "contracts",
        "margin",
        "commission",
        "profit_usd",
        "win_loss",
        "strategy",
        "market_session",
    ]
    # Return only the columns we want to keep
    df = df[[col for col in columns_to_keep if col in df.columns]]

    return df


def write_to_postgresql(df: pd.DataFrame, session: SQLAlchemySession) -> None:
    # Add trades to the database
    for _, row in df.iterrows():
        trade = Trade(
            symbol=row["symbol"],
            type=row["type"],
            date=row["date"],
            time=row["time"],
            day=row["day"],
            hour=row["hour"],
            minute=row["minute"],
            weekday=row["weekday"],
            weeknum=row["weeknum"],
            month=row["month"],
            year=row["year"],
            contracts=row["contracts"],
            margin=row["margin"],
            commission=row["commission"],
            profit_usd=row["profit_usd"],
            win_loss=row["win_loss"],
            strategy=row["strategy"],
            market_session=row["market_session"],
        )
        try:
            # Check if the trade already exists
            existing_trade = (
                session.query(Trade)
                .filter_by(
                    symbol=trade.symbol,
                    type=trade.type,
                    date=trade.date,
                    time=trade.time,
                    contracts=trade.contracts,
                    profit_usd=trade.profit_usd,
                )
                .first()
            )
            # Insert the trade if it doesn't exist
            if existing_trade is None:
                session.add(trade)
                session.commit()
            else:
                print(f"Trade already exists: {trade}")
        # Rollback the session if an exception occurs
        except IntegrityError:
            session.rollback()
            print(f"Error inserting trade: {trade}")


def main() -> None:
    session = Session()

    try:
        # Part 1: Data Ingestion and Processing
        directory = "/Users/charlesobrien/Desktop/Tradingview-Files/Mini"

        try:
            # First, try to find today's file
            file_path = find_latest_file(directory)
        except FileNotFoundError:
            print("No file found for today's date. Searching for recent files...")
            # If today's file is not found, search for files from the last 7 days
            seven_days_ago = datetime.now().date() - timedelta(days=7)
            file_path = find_file_in_date_range(directory, start_date=seven_days_ago)

        print(f"Processing file: {file_path}")

        df = load_csv_to_dataframe(file_path)
        cleaned_df = clean_and_transform_data(df)
        write_to_postgresql(cleaned_df, session)

        print("Data ingestion and processing complete.")

        # Part 2: Data Analytics and Visualization would follow here...

    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("No suitable files found for processing.")
    except pd.errors.EmptyDataError:
        print("Error: The CSV file is empty.")
    except pd.errors.ParserError as e:
        print(f"Error parsing CSV file: {e}")
    except SQLAlchemyError as e:
        print(f"Database error: {e}")
        session.rollback()
    except psycopg2.Error as e:
        print(f"PostgreSQL error: {e}")
    except ValueError as e:
        print(f"Value error: {e}")
    except IOError as e:
        print(f"I/O error: {e}")
    finally:
        session.close()


if __name__ == "__main__":
    main()
