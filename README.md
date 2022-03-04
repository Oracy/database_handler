# Database Handler class to facilitate

## How to use?

- Create file on `home` folder

```bash
vi ~/access_information

{
    "connection_name": {
        "host": "domain.net",
        "user": "domain\\user",
        "password": "pwd"
    }
}
```

```python
from utils import get_db_data
from DatabaseHandler import DatabaseHandler
import pandas as pd

# Load get_db_data function
db_access = get_db_data()

# Load data from access_information
database_handler = DatabaseHandler(db_access["connection_name"])

# Create query
query = """
SELECT * FROM database.table;
"""

# Run query and save result to res
res = pd.DataFrame(database_handler.fetch(query))

# Show result
res
```