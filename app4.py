import pyodbc
import json
import sys
import decimal
from datetime import date, datetime

# Configuration for database connection
config = {
    "trusted_connection": "yes",
    "trustServerCertificate": "yes"
}

pool = None

def check_odbc_driver(driver_name):
    installed_drivers = pyodbc.drivers()
    return driver_name in installed_drivers

def get_connection_string(config):
    if 'username' in config and 'password' in config and config['username'] and config['password']:
        return (
            f"Driver={{{config['driver']}}};"
            f"Server={config['server']};"
            f"Database={config['database']};"
            f"UID={config['username']};"
            f"PWD={config['password']};"
            "TrustServerCertificate=yes;"
        )
    else:
        return (
            f"Driver={{{config['driver']}}};"
            f"Server={config['server']};"
            f"Database={config['database']};"
            "Trusted_Connection=yes;"
            "TrustServerCertificate=yes;"
        )

def connect_to_database():
    global pool
    try:
        if not pool:
            connection_string = get_connection_string(config)
            print("Connecting to database...")
            pool = pyodbc.connect(connection_string)
        return pool
    except Exception as err:
        print(f"Database connection error: {err}")
        raise err

def get_connection():
    global pool
    try:
        if not pool:
            connect_to_database()
        return pool
    except Exception as err:
        print(f"Error getting connection: {err}")
        raise err

def fetch_data(query, batch_size=1000, is_stored_procedure=False, placeholder_count=3):
    """Fetch data from the database, handling stored procedures and temporary tables."""
    connection = get_connection()
    try:
        cursor = connection.cursor()
        print("Executing query...")

        # Execute with SET NOCOUNT ON to improve pyodbc handling of temp tables
        cursor.execute("SET NOCOUNT ON; " + query)

        # Retrieve column names dynamically
        columns = cursor.description
        if not columns:
            print("No columns found; likely an empty result set.")
            return []  # No columns and no rows

        column_names = [column[0] for column in columns]
        # print(f"Detected columns: {column_names}")

        results = []
        # Fetch rows in batches to avoid memory issues with large data sets
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            for row in rows:
                results.append(dict(zip(column_names, row)))

        # If no results but columns exist, generate dynamic placeholders
        if not results and column_names:
            print("No rows returned; generating placeholder data.")
            placeholder = {col: 0 for col in column_names}  # Default value of 0 for each column
            results = [placeholder] * placeholder_count  # Repeat placeholder

        # print(f"Fetched data: {results}")
        return results
    except Exception as err:
        print(f"Error fetching data: {err}")
        raise err
    finally:
        cursor.close()
        connection.close()

def handle_output(query, key=False, file_path=None, is_stored_procedure=False, placeholder_count=3):
    """Handle the output depending on the key and file path, with dynamic placeholder support."""
    try:
        results = fetch_data(query, is_stored_procedure=is_stored_procedure, placeholder_count=placeholder_count)
        
        if key:
            print(json.dumps({"isSuccess": True, "results": results}, cls=CustomJSONEncoder))
        elif file_path:
            with open(file_path, mode='w', encoding='utf-8') as file:
                json.dump(results, file, cls=CustomJSONEncoder)
            print(json.dumps({"isSuccess": True, "results": file_path}, cls=CustomJSONEncoder))
        else:
            print(json.dumps({"isSuccess": True}, cls=CustomJSONEncoder))
    except Exception as err:
        print(json.dumps({"isSuccess": False, "error": str(err)}, cls=CustomJSONEncoder))
        raise err

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        return super(CustomJSONEncoder, self).default(obj)

if __name__ == "__main__":
    try:
        driver_version = sys.argv[1]
        server_name = sys.argv[2]
        db_name = sys.argv[3]
        sql_file_path = sys.argv[4]

        key = sys.argv[5].lower() == 'true' if len(sys.argv) > 5 else False
        file_path = sys.argv[6] if len(sys.argv) > 6 and sys.argv[6].endswith('.json') else None

        if driver_version == '18':
            config['driver'] = 'ODBC Driver 18 for SQL Server'
        elif driver_version == '17':
            config['driver'] = 'ODBC Driver 17 for SQL Server'
        else:
            print(json.dumps({"error": "Unsupported driver version", "isSuccess": False}))
            sys.exit(1)

        config['server'] = server_name
        config['database'] = db_name

        if len(sys.argv) > 7:
            config['username'] = sys.argv[7]
            config['password'] = sys.argv[8]
        else:
            config['username'] = None
            config['password'] = None

        driver_name = config['driver']
        if not check_odbc_driver(driver_name):
            print(json.dumps({"error": f"{driver_name} not found", "isSuccess": False}))
            sys.exit(1)

        with open(sql_file_path, 'r') as file:
            query = file.read()

        is_stored_procedure = query.strip().upper().startswith('EXEC')

        if file_path:
            handle_output(query, key=key, file_path=file_path, is_stored_procedure=is_stored_procedure)
        else:
            handle_output(query, key=key, file_path=file_path, is_stored_procedure=is_stored_procedure)

    except Exception as e:
        print(json.dumps({"error": str(e), "isSuccess": False}))
        sys.exit(1)
