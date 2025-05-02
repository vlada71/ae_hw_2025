from gettext import Catalog
import os
import pyarrow as pa
import requests
from aws_secret_manager import get_secret
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from ast import literal_eval
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import TableAlreadyExistsError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
from pyiceberg.types import FloatType, LongType, NestedField, StringType, TimestampType, DoubleType

load_dotenv()

def get_polygon_data(ticker, start_date, end_date, api_key):
    """Fetch stock data from Polygon API for a specific ticker and date range"""
    polygon_url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}?adjusted=true&sort=asc&apiKey={api_key}"
    response = requests.get(polygon_url)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data for {ticker}: {response.status_code}")
        return None

def process_stock_data(ticker, polygon_response):
    """Process Polygon API response into dataframe format"""
    if not polygon_response or 'results' not in polygon_response:
        return pd.DataFrame()
    
    results = polygon_response['results']
    rows = []
    
    for result in results:
        timestamp = datetime.fromtimestamp(result['t'] / 1000)
        row = {
            'ticker': ticker,
            'date': timestamp,
            'open': result['o'],
            'high': result['h'],
            'low': result['l'],
            'close': result['c'],
            'volume': result['v'],
            'vwap': result.get('vw', None),
            'transactions': result.get('n', None)
        }
        rows.append(row)
    
    return rows

def create_first_table_snapshot(table, stock_schema):
    null_row = {
        "ticker": None,
        "date": datetime.now(timezone.utc),
        "open": None,
        "high": None,
        "low": None,
        "close": None,
        "volume": None,
        "vwap": None,
        "transactions":None
    }

    arrow_table = pa.Table.from_pylist([null_row], schema=stock_schema.as_arrow())
    table.overwrite(arrow_table)

def get_stock_table(catalog, schema_name, stock_schema, table_name):
    """Create the daily partitioned Iceberg table for stock data if it does not exist or return the tablr"""
    # Define partition spec (daily partitioning by date)
    partition_spec = PartitionSpec(
        PartitionField(source_id=2, transform=DayTransform(), name="date_day", field_id=1000 )
    )
    
    table_identifier = f"{schema_name}.{table_name}"
    try:
        table = catalog.create_table(
            identifier=table_identifier,
            schema=stock_schema,
            partition_spec=partition_spec
        )

    except TableAlreadyExistsError:
        table = catalog.load_table(table_identifier)
    
    return table

def homework_script(start_date=None, end_date=None):
    """Main script to fetch MAANG stock data and load it into Iceberg"""
    # Default to yesterday if no dates provided
    if not start_date:
        yesterday = datetime.now() - timedelta(days=1)
        start_date = yesterday.strftime("%Y-%m-%d")
    
    if not end_date:
        end_date = start_date
    
    print(f"Processing data for date range: {start_date} to {end_date}")
    
    # Stock tickers for MAANG companies
    maang_stocks = ['AAPL', 'AMZN', 'NFLX', 'GOOGL', 'META']
    
    # Get API key from AWS Secret Manager
    polygon_api_key = literal_eval(get_secret("POLYGON_CREDENTIALS"))['AWS_SECRET_ACCESS_KEY']
    
    # Get catalog name and credentials from AWS Secret Manager
    catalog_name = get_secret("CATALOG_NAME")
    tabular_credential = get_secret('TABULAR_CREDENTIAL')
    # Use environment variable for schema or default to current user
    schema_name = os.getenv("SCHEMA")

        # Connect to catalog
    catalog = load_catalog(
        'academy',
        type="rest",  # Explicitly define the catalog type as REST
        uri="https://api.tabular.io/ws",  # Your REST catalog endpoint
        warehouse=get_secret("CATALOG_NAME"),
        credential=get_secret('TABULAR_CREDENTIAL')
    )
    
    # Define schema for the table
    stock_schema = Schema(
        NestedField(1, "ticker", StringType(), required=True),
        NestedField(2, "date", TimestampType(), required=True),
        NestedField(3, "open", DoubleType(), required=True),
        NestedField(4, "high", DoubleType(), required=True),
        NestedField(5, "low", DoubleType(), required=True),
        NestedField(6, "close", DoubleType(), required=True),
        NestedField(7, "volume", LongType(), required=True),
        NestedField(8, "vwap", DoubleType()),
        NestedField(9, "transactions", LongType())
    )

    table = get_stock_table(catalog, schema_name, stock_schema, "stock_prices")
   
    if not table.snapshots():
        create_first_table_snapshot(table, stock_schema)

    branch_name = "audit_branch"
    current_snapshot_id = table.snapshots()[-1].snapshot_id
    with table.manage_snapshots() as ms:
        (
            ms.create_branch(
                snapshot_id=current_snapshot_id, branch_name=branch_name
            ).create_tag(
                snapshot_id=current_snapshot_id, tag_name="Creating audit_branch"
            )
        )

    table_identifier = f"{schema_name}.stock_prices"
    table = catalog.load_table(table_identifier)
    
    # Fetch and process data for each stock
    for ticker in maang_stocks:
        print(f"Fetching data for {ticker}")
        response = get_polygon_data(ticker, start_date, end_date, polygon_api_key)
        rows = process_stock_data(ticker, response)
        arrow_table = pa.Table.from_pylist(rows, schema=stock_schema.as_arrow())
        table.overwrite(arrow_table)


if __name__ == "__main__":
    # Can be called with specific dates or defaults to yesterday
    # Example: python3 stock_prices.py 2025-01-05 2025-01-06
    import sys
    
    if len(sys.argv) > 2:
        homework_script(sys.argv[1], sys.argv[2])
    elif len(sys.argv) > 1:
        homework_script(sys.argv[1])
    else:
        homework_script()
