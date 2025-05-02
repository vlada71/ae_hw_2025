from aws_secret_manager import get_secret
from pyiceberg.catalog import load_catalog

import pyarrow as pa
from aws_secret_manager import get_secret
from pyiceberg.catalog import load_catalog
from dotenv import load_dotenv
from ast import literal_eval
load_dotenv()

# Print details for debugging (don't include in production code)
credential = get_secret('TABULAR_CREDENTIAL')
warehouse = get_secret("CATALOG_NAME")
print(f"Warehouse: {warehouse}")
print(f"Credential format looks valid: {bool(credential) and len(credential) > 10}")
print(f"credential: {credential}")
#Try to connect
catalog = load_catalog(
    'academy',
    type="rest",
    uri="https://api.tabular.io/ws",
    warehouse=warehouse,
    credential=credential
)

# Test if connected
print("Available namespaces:")
print(catalog.list_namespaces())