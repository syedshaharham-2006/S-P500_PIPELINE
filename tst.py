from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from botocore.config import Config
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, SSLError, ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import requests
import yfinance as yf
import logging
import pytz
import os
from io import StringIO

logger = logging.getLogger("airflow.task")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["youremail@example.com"],
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "sp500_all_1min_enhanced_pipeline",
    default_args=default_args,
    description="All S&P500 1-min OHLCV with S3 & Snowflake",
    schedule_interval="0 21 * * 1-5",
    start_date=datetime(2025, 11, 6),
    catchup=False,
    tags=["sp500", "intraday", "snowflake", "s3", "enhanced"],
    max_active_runs=1,
)

def previous_trading_day_ny():
    tz = pytz.timezone("America/New_York")
    now = datetime.now(tz)
    wd = now.weekday()
    if wd == 0:
        return now - timedelta(days=3)
    elif wd == 6:
        return now - timedelta(days=2)
    elif wd == 5:
        return now - timedelta(days=1)
    else:
        return now - timedelta(days=1)

def fetch_sp500_tickers(**context):
    try:
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        headers = {'User-Agent': 'Mozilla/5.0'}
        html = requests.get(url, headers=headers, timeout=30).text
        df = pd.read_html(html)[0]
        sp500_tickers = df['Symbol'].str.replace('.', '-', regex=False).tolist()
        context['ti'].xcom_push(key="tickers", value=sp500_tickers)
        logger.info(f"✓ Fetched {len(sp500_tickers)} S&P 500 tickers.")
    except Exception as e:
        logger.error(f"Failed to fetch S&P 500 tickers: {str(e)}")
        raise

def fetch_1min_data(**context):
    try:
        tickers = context["ti"].xcom_pull(key="tickers", task_ids="fetch_sp500_tickers")
        prev_trading_day = previous_trading_day_ny()
        day_str = prev_trading_day.strftime("%Y-%m-%d")
        
        def fetch_ticker_data(symbol):
            try:
                df = yf.download(
                    symbol,
                    start=day_str,
                    end=(prev_trading_day + timedelta(days=1)).strftime("%Y-%m-%d"),
                    interval='1m',
                    progress=False,
                    auto_adjust=True
                )
                if not df.empty:
                    # Ensure timestamps are in US/Eastern
                    if df.index.tz is None:
                        df.index = df.index.tz_localize('UTC')
                    df.index = df.index.tz_convert('America/New_York')
                    # Filter only US market hours
                    df = df.between_time('09:30', '16:00')
                    df.reset_index(inplace=True)
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = ['_'.join([str(i) for i in col]).rstrip('_') for col in df.columns.values]
                    rename_map = {}
                    for col in df.columns:
                        base = col.split('_')[0] if '_' in col else col
                        if base in ['Open', 'High', 'Low', 'Close', 'Volume']:
                            rename_map[col] = base
                    df.rename(columns=rename_map, inplace=True)
                    for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
                        if col not in df.columns:
                            df[col] = None
                    df['Symbol'] = symbol
                    df.rename(columns={'Datetime': 'Datetime'}, inplace=True)
                    return symbol, df
                else:
                    return symbol, None
            except Exception as exc:
                logger.warning(f"{symbol}: fetch failed ({exc})")
                return symbol, None
        
        results = {}
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_ticker = {executor.submit(fetch_ticker_data, ticker): ticker for ticker in tickers}
            for future in as_completed(future_to_ticker):
                symbol, data = future.result()
                if data is not None:
                    results[symbol] = data

        if not results:
            raise ValueError(f"No 1-minute data retrieved for {day_str}!")

        # FIX: Ensure unique index for each DataFrame before concat
        frames = []
        for df in results.values():
            if df is not None:
                df = df.reset_index(drop=True)  # Ensure unique index for each DataFrame
                frames.append(df)

        # Concatenate the frames with a unique index
        combined_df = pd.concat(frames, ignore_index=True)
        context["ti"].xcom_push(key="raw_data", value=combined_df.to_json(date_format='iso', orient='records'))
    except Exception as e:
        logger.error(f"Failed to fetch 1-min data: {str(e)}")
        raise

def transform_data(**context):
    try:
        raw_json = context["ti"].xcom_pull(key="raw_data", task_ids="fetch_1min_data")
        df = pd.read_json(StringIO(raw_json))
        
        for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
            if col not in df.columns:
                logger.error(f"Missing column: {col}")
                raise ValueError(f"Required column not found: {col}")

        if 'Symbol' not in df.columns:
            logger.error("Missing column: Symbol")
            raise ValueError("Required column not found: Symbol")

        datetime_col = 'Datetime' if 'Datetime' in df.columns else df.columns[0]
        df[datetime_col] = pd.to_datetime(df[datetime_col])

        if df[datetime_col].dt.tz is None:
            df[datetime_col] = df[datetime_col].dt.tz_localize('UTC')
        
        df[datetime_col] = df[datetime_col].dt.tz_convert('America/New_York')
        df = df.sort_values(['Symbol', datetime_col]).reset_index(drop=True)
        
        for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
            df[col] = df.groupby('Symbol')[col].fillna(method='ffill')
        
        df['minute_returns'] = df.groupby('Symbol')['Close'].pct_change() * 100
        df['minute_returns'] = df['minute_returns'].fillna(0)
        df['trading_hours'] = df[datetime_col].dt.time
        df['rolling_avg'] = df.groupby('Symbol')['Close'].transform(
            lambda x: x.rolling(window=5, min_periods=1).mean()
        )
        df['trade_date'] = df[datetime_col].dt.date
        df['close_change'] = df.groupby('Symbol')['Close'].diff().fillna(0)
        df['volume_change'] = df.groupby('Symbol')['Volume'].diff().fillna(0)
        df = df.rename(columns={'Symbol': 'symbol'})
        df['Datetime'] = df[datetime_col]
        
        context["ti"].xcom_push(key="transformed_data", value=df.to_json(date_format='iso', orient='records'))
    except Exception as e:
        logger.error(f"Failed during transformation: {str(e)}")
        raise

def upload_to_s3(**context):
    try:
        # Fetching the bucket and key prefix from Airflow variables
        S3_BUCKET = Variable.get("S3_BUCKET")  # Example: 's3forairflowproject'
        S3_KEY_PREFIX = Variable.get("S3_KEY_PREFIX", default_var="sp500/")

        # Get the AWS connection and client
        aws_hook = AwsBaseHook(aws_conn_id="aws_connection", client_type="s3")
        s3 = aws_hook.get_client_type("s3", region_name='ap-south-1')  # Mumbai region (ap-south-1)

        # Retrieving the transformed data from the previous task
        transformed_json = context["ti"].xcom_pull(key="transformed_data", task_ids="transform_data")
        df = pd.read_json(StringIO(transformed_json))

        # Preparing the file name with date and timestamp
        prev = previous_trading_day_ny()
        date_str = prev.strftime('%Y%m%d')
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        key = f"{S3_KEY_PREFIX}sp500_all_1min_{date_str}_{ts}.csv"

        # Upload the data to S3
        body = df.to_csv(index=False).encode()
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body)

        # Push the path to XCom for further processing
        context["ti"].xcom_push(key="s3_path", value=f"s3://{S3_BUCKET}/{key}")

        logging.info(f"Successfully uploaded file to {S3_BUCKET}/{key}")

    except NoCredentialsError:
        logging.error("No AWS credentials found. Please check your AWS connection configuration.")
        raise

    except PartialCredentialsError:
        logging.error("Incomplete AWS credentials found. Please ensure that all required credentials are set.")
        raise

    except SSLError as ssl_error:
        logging.error(f"SSL Error: {ssl_error}. Please check the SSL certificate or endpoint settings.")
        raise

    except ClientError as client_error:
        logging.error(f"ClientError: {client_error}. There might be a permission issue or invalid request.")
        raise

    except Exception as e:
        logging.error(f"Unexpected error occurred: {e}")
        raise



def load_to_snowflake(**context):
    try:
        transformed_json = context["ti"].xcom_pull(key="transformed_data", task_ids="transform_data")
        df = pd.read_json(StringIO(transformed_json))
        hook = SnowflakeHook(snowflake_conn_id="snowflake_connection")
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute(""" 
            CREATE TABLE IF NOT EXISTS SP500_1MIN_PRICES (
                DATETIME TIMESTAMP_TZ,
                OPEN FLOAT,
                HIGH FLOAT,
                LOW FLOAT,
                CLOSE FLOAT,
                VOLUME FLOAT,
                SYMBOL VARCHAR(10),
                MINUTE_RETURNS FLOAT,
                TRADING_HOURS TIME,
                ROLLING_AVG FLOAT,
                TRADE_DATE DATE,
                CLOSE_CHANGE FLOAT,
                VOLUME_CHANGE FLOAT,
                LOAD_TIMESTAMP TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
            )
            CLUSTER BY (TRADE_DATE, SYMBOL);
        """)

        df['Datetime'] = pd.to_datetime(df['Datetime'])
        df['Datetime'] = df['Datetime'].dt.tz_localize("America/New_York", ambiguous='NaT', nonexistent='NaT') if df['Datetime'].dt.tz is None else df['Datetime']
        df['Datetime'] = df['Datetime'].dt.strftime('%Y-%m-%d %H:%M:%S%z')
        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')
        df['trading_hours'] = pd.to_datetime(df['trading_hours'], format='%H:%M:%S').dt.strftime('%H:%M:%S')

        insert_columns = [
            'Datetime', 'Open', 'High', 'Low', 'Close', 'Volume',
            'symbol', 'minute_returns', 'trading_hours', 'rolling_avg',
            'trade_date', 'close_change', 'volume_change'
        ]
        insert_query = """
            INSERT INTO SP500_1MIN_PRICES
            (DATETIME, OPEN, HIGH, LOW, CLOSE, VOLUME, SYMBOL,
             MINUTE_RETURNS, TRADING_HOURS, ROLLING_AVG, TRADE_DATE,
             CLOSE_CHANGE, VOLUME_CHANGE)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        data = [tuple(x) for x in df[insert_columns].to_numpy()]
        cursor.executemany(insert_query, data)
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Failed to load data to Snowflake: {str(e)}")
        raise

# Define tasks
t1 = PythonOperator(
    task_id="fetch_sp500_tickers",
    python_callable=fetch_sp500_tickers,
    dag=dag,
)
t2 = PythonOperator(
    task_id="fetch_1min_data",
    python_callable=fetch_1min_data,
    dag=dag,
)
t3 = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    dag=dag,
)
t4 = PythonOperator(
    task_id="upload_to_s3",
    python_callable=upload_to_s3,
    dag=dag,
)
t5 = PythonOperator(
    task_id="load_to_snowflake",
    python_callable=load_to_snowflake,
    dag=dag,
)

t1 >> t2 >> t3 >> [t4, t5]