import sys
from unittest.mock import MagicMock

# Mock dlt module
mock_dlt = MagicMock()

# Create table decorator mock
def table_decorator(*args, **kwargs):
    def decorator(func):
        return func
    return decorator

# Add mock methods
mock_dlt.table = table_decorator
mock_dlt.read = MagicMock()
mock_dlt.read_stream = MagicMock()

# Replace real dlt with mock
sys.modules['dlt'] = mock_dlt
