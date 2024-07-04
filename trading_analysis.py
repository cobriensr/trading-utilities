"""Standardized Trading Analysis for LuxAlgo® Backtesting System™ (S&O)"""

import logging
from typing import Dict, List, Any
import pandas as pd
import plotly.graph_objs as go
import plotly.express as px
from sqlalchemy import create_engine, text
import dash
from dash import dcc, html, Input, Output, dash_table
from dash.dash_table.Format import Format, Scheme

# pylint: disable=missing-function-docstring, missing-class-docstring, trailing-whitespace, line-too-long, missing-final-newline, logging-fstring-interpolation, broad-exception-caught, redefined-outer-name

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Constants
INITIAL_CAPITAL = 20000
DB_URL = "postgresql://charlesobrien:password@localhost:5432/trading_db"


def load_data() -> pd.DataFrame:
    """Load and preprocess data from the database."""
    try:
        engine = create_engine(DB_URL)
        query = text("SELECT * FROM trades ORDER BY date DESC, time DESC")
        df = pd.read_sql(query, engine)

        df["date"] = pd.to_datetime(df["date"])
        numeric_columns = ["contracts", "margin", "commission", "profit_usd"]
        df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors="coerce")

        df["cumulative_profit"] = df["profit_usd"].cumsum()
        df["equity"] = INITIAL_CAPITAL + df["cumulative_profit"]

        # Extract day and hour from date and time
        df["day"] = df["date"].dt.day_name()

        # Handle 'time' column based on its data type
        if df["time"].dtype == "object":
            df["hour"] = pd.to_datetime(
                df["time"], format="%H:%M:%S", errors="coerce"
            ).dt.hour
        elif df["time"].dtype.name == "time":
            df["hour"] = df["time"].apply(lambda x: x.hour if pd.notnull(x) else pd.NaT)
        else:
            logger.warning(
                f"Unexpected data type for 'time' column: {df['time'].dtype}"
            )
            df["hour"] = pd.NaT

        # Create a custom order for days starting with Monday
        day_order = [
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
            "Sunday",
        ]
        df["day"] = pd.Categorical(df["day"], categories=day_order, ordered=True)

        logger.info("Data loaded and preprocessed successfully")
        return df
    except Exception as e:
        logger.exception(f"Error loading data: {e}")
        return pd.DataFrame()


def create_day_hour_performance_table(df: pd.DataFrame) -> pd.DataFrame:
    """Create a performance table that combines day and hour information."""
    # Convert 'day' from Categorical to string
    df["day_str"] = df["day"].astype(str)
    df["day_hour"] = df["day_str"] + " " + df["hour"].astype(str).str.zfill(2) + ":00"

    # Use observed=True to address the FutureWarning
    group = df.groupby(["day", "hour", "day_hour"], observed=True)
    stats = group["profit_usd"].agg(["mean", "sum", "count"]).reset_index()
    stats = stats.rename(
        columns={"mean": "avg_profit", "sum": "total_profit", "count": "trade_count"}
    )
    stats["win_rate"] = (
        group["profit_usd"].apply(lambda x: (x > 0).mean()).reset_index(drop=True)
    )
    stats = stats.round(2)

    # Sort by day and hour
    day_order = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]
    stats["day"] = pd.Categorical(stats["day"], categories=day_order, ordered=True)
    stats = stats.sort_values(["day", "hour"])

    return stats


def create_layout(df: pd.DataFrame, day_hour_performance: pd.DataFrame) -> html.Div:
    """Create the Dash app layout with day-hour performance table and dropdown menus using Dash components."""

    days = sorted(day_hour_performance["day"].unique())
    hours = sorted(day_hour_performance["hour"].unique())

    layout = [
        html.H1("Trading Dashboard"),
        dcc.Graph(id="equity-curve"),
        html.H2("Performance Analysis - Day and Hour"),
        html.Div(
            [
                dcc.Dropdown(
                    id="day-dropdown",
                    options=[{"label": day, "value": day} for day in days],
                    value=days,
                    multi=True,
                ),
                dcc.Dropdown(
                    id="hour-dropdown",
                    options=[{"label": str(hour), "value": hour} for hour in hours],
                    value=hours,
                    multi=True,
                ),
            ],
            style={
                "width": "50%",
                "display": "inline-block",
                "vertical-align": "middle",
            },
        ),
        dash_table.DataTable(
            id="day-hour-performance-table",
            columns=[
                {"name": "Day", "id": "day"},
                {"name": "Hour", "id": "hour"},
                {"name": "Day-Hour", "id": "day_hour"},
                {
                    "name": "Avg Profit",
                    "id": "avg_profit",
                    "type": "numeric",
                    "format": Format(precision=2, scheme=Scheme.fixed),
                },
                {
                    "name": "Total Profit",
                    "id": "total_profit",
                    "type": "numeric",
                    "format": Format(precision=2, scheme=Scheme.fixed),
                },
                {"name": "Trade Count", "id": "trade_count"},
                {
                    "name": "Win Rate",
                    "id": "win_rate",
                    "type": "numeric",
                    "format": Format(precision=2, scheme=Scheme.percentage),
                },
            ],
            data=day_hour_performance.to_dict("records"),
            filter_action="native",
            page_size=20,
            style_table={"overflowX": "auto"},
            style_cell={"textAlign": "left", "padding": "5px"},
            style_header={"backgroundColor": "lightgrey", "fontWeight": "bold"},
            style_data_conditional=[
                {
                    "if": {"column_id": col, "filter_query": f"{{{col}}} > 0"},
                    "backgroundColor": "#CCFFCC",
                    "color": "green",
                }
                for col in ["avg_profit", "total_profit"]
            ]
            + [
                {
                    "if": {"column_id": col, "filter_query": f"{{{col}}} < 0"},
                    "backgroundColor": "#FFCCCC",
                    "color": "red",
                }
                for col in ["avg_profit", "total_profit"]
            ],
        ),
        html.H2("Trade Details"),
        dash_table.DataTable(
            id="trade-table",
            columns=[{"name": i, "id": i} for i in df.columns],
            data=df.to_dict("records"),
            filter_action="native",
            sort_action="native",
            sort_mode="multi",
            page_size=10,
            style_table={"overflowX": "auto"},
            style_cell={"textAlign": "left", "padding": "5px"},
            style_header={"backgroundColor": "lightgrey", "fontWeight": "bold"},
            style_data_conditional=[
                {"if": {"row_index": "odd"}, "backgroundColor": "rgb(248, 248, 248)"}
            ],
        ),
    ]
    return html.Div(layout)


def create_app(df: pd.DataFrame) -> dash.Dash:
    """Create and configure the Dash app."""
    day_hour_performance = create_day_hour_performance_table(df)
    app = dash.Dash(__name__)
    app.layout = create_layout(df, day_hour_performance)

    @app.callback(
        Output("day-hour-performance-table", "data"),
        [Input("day-dropdown", "value"), Input("hour-dropdown", "value")],
    )
    def update_performance_table(selected_days, selected_hours):
        filtered_data = day_hour_performance[
            (day_hour_performance["day"].isin(selected_days))
            & (day_hour_performance["hour"].isin(selected_hours))
        ]
        return filtered_data.to_dict("records")

    @app.callback(
        Output("equity-curve", "figure"), Input("trade-table", "derived_virtual_data")
    )
    def update_graph(rows: List[Dict[str, Any]]) -> go.Figure:
        if not rows:
            return go.Figure()

        try:
            dff = pd.DataFrame(rows)
            required_columns = ["date", "profit_usd"]
            if not all(col in dff.columns for col in required_columns):
                logger.error(
                    f"Missing required columns. Available columns: {dff.columns}"
                )
                return go.Figure()

            dff["date"] = pd.to_datetime(dff["date"], errors="coerce")
            dff["profit_usd"] = pd.to_numeric(dff["profit_usd"], errors="coerce")
            dff = dff.sort_values("date").reset_index(drop=True)

            dff["cumulative_profit"] = dff["profit_usd"].cumsum()
            dff["equity"] = INITIAL_CAPITAL + dff["cumulative_profit"]

            fig = px.line(dff, x="date", y="equity", title="Equity Curve")
            fig.add_hline(
                y=INITIAL_CAPITAL,
                line_dash="dash",
                line_color="red",
                annotation_text="Initial Capital",
            )
            return fig
        except Exception as e:
            logger.exception(f"Error in update_graph: {e}")
            return go.Figure()

    return app


if __name__ == "__main__":
    df = load_data()
    if not df.empty:
        app = create_app(df)
        app.run_server(debug=True, dev_tools_props_check=False)
    else:
        logger.error("Failed to load data. Application not started.")
