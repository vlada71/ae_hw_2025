import pyarrow as pa
from aws_secret_manager import get_secret
from pyiceberg.catalog import load_catalog
from dotenv import load_dotenv
from ast import literal_eval
load_dotenv()

def homework_script():
    maang_stocks = ['AAPL', 'AMZN', 'NFLX', 'GOOGL', 'META']
    polygon_api_key = literal_eval(get_secret("POLYGON_CREDENTIALS"))['AWS_SECRET_ACCESS_KEY']
    polygon_url = "https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/2025-01-05/2025-01-06?adjusted=true&sort=asc&apiKey="
    catalog = load_catalog(
        'academy',
        type="rest",  # Explicitly define the catalog type as REST
        uri="https://api.tabular.io/ws",  # Your REST catalog endpoint
        warehouse=get_secret("CATALOG_NAME"),
        credential=get_secret('TABULAR_CREDENTIAL'),
    )
    # table = catalog.load_table('bootcamp.nba_player_seasons')
    ## Todo create your partitioned Iceberg table
    ## catalog.create_table()
    ## TODO create a branch table.create_branch()
    ## TODO read data from the Polygon API and load it into the Iceberg table branch

if __name__ == "__main__":
    homework_script()
