import pyodbc
import json
import os
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
    """ Check if the specified ODBC driver is installed """
    installed_drivers = pyodbc.drivers()
    return driver_name in installed_drivers

def get_connection_string(config):
    """ Generate a connection string from the config """
    if 'username' in config and 'password' in config and config['username'] and config['password']:
        # Connection string for username/password-based authentication
        return (
            f"Driver={{{config['driver']}}};"
            f"Server={config['server']};"
            f"Database={config['database']};"
            f"UID={config['username']};"
            f"PWD={config['password']};"
            "TrustServerCertificate=yes;"
        )
    else:
        # Trusted connection string
        return (
            f"Driver={{{config['driver']}}};"
            f"Server={config['server']};"
            f"Database={config['database']};"
            "Trusted_Connection=yes;"
            "TrustServerCertificate=yes;"
        )

def connect_to_database():
    """ Establish a connection to the database """
    global pool
    try:
        if not pool:
            connection_string = get_connection_string(config)
            pool = pyodbc.connect(connection_string)
        return pool
    except Exception as err:
        raise err

def get_connection():
    """ Get the current database connection """
    global pool
    try:
        if not pool:
            connect_to_database()
        if pool:
            pool.close()
        pool = pyodbc.connect(get_connection_string(config))
        return pool
    except Exception as err:
        raise err

def fetch_data(query, batch_size=1000, is_stored_procedure=False):
    """Fetch data from the database."""
    connection = get_connection()
    try:
        cursor = connection.cursor()
        
        if is_stored_procedure:
            cursor.execute(query)
        else:
            cursor.execute(query)

        # Check if there is a result set
        columns = cursor.description
        if not columns:
            return None
        
        results = []
        columns = [column[0] for column in cursor.description]
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            for row in rows:
                results.append(dict(zip(columns, row)))
        return results
    except Exception as err:
        raise err
    finally:
        cursor.close()
        connection.close()

def execute_query(query, is_stored_procedure=False):
    """Execute DML/DDL queries."""
    connection = get_connection()
    try:
        cursor = connection.cursor()
        
        if is_stored_procedure:
            cursor.execute(query)
        else:
            cursor.execute(query)

        connection.commit()
        return True
    except Exception as err:
        raise err
    finally:
        cursor.close()
        connection.close()

def fetch_data_and_write_json(query, file_path, batch_size=1000, is_stored_procedure=False, key=False):
    """ Fetch data and write to JSON, handling stored procedures if necessary """
    connection = get_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(query)

        json_filename = file_path
        if os.path.exists(json_filename):
            os.remove(json_filename)

        results = []
        has_data = False

        while True:
            columns = cursor.description
            if columns:
                while True:
                    rows = cursor.fetchmany(batch_size)
                    if rows:
                        has_data = True
                        columns = [column[0] for column in cursor.description]
                        for row in rows:
                            results.append(dict(zip(columns, row)))
                    else:
                        break
            if not cursor.nextset():
                break

        if has_data and key is False and file_path:
            with open(json_filename, mode='w', encoding='utf-8') as file:
                json.dump(results, file, cls=CustomJSONEncoder)  # Use CustomJSONEncoder here
            print(json.dumps({"isSuccess": True, "results": file_path}, cls=CustomJSONEncoder))
        elif key is True and has_data:
            print(json.dumps({"isSuccess": True, "results": results}, cls=CustomJSONEncoder))
        else:
            print(json.dumps({"isSuccess": True, "results": []}))

        return json_filename
    except Exception as err:
        connection.rollback()
        raise err
    finally:
        cursor.close()
        connection.close()

def handle_output(query, key=False, file_path=None, is_stored_procedure=False):
    """Handle the output depending on the key and file path."""
    try:
        if key:
            # Fetch and print the data when key is True
            results = fetch_data(query, is_stored_procedure=is_stored_procedure)
            if results:
                print(json.dumps({"isSuccess": True, "results": results}, cls=CustomJSONEncoder))
            else:
                print(json.dumps({"isSuccess": False, "message": "No data returned from the query."}, cls=CustomJSONEncoder))
        elif file_path:
            # Fetch data and save to a JSON file when file path is provided
            results = fetch_data(query, is_stored_procedure=is_stored_procedure)
            if results:
                with open(file_path, mode='w', encoding='utf-8') as file:
                    json.dump(results, file, cls=CustomJSONEncoder)  # Use CustomJSONEncoder here
                print(json.dumps({"isSuccess": True, "results": file_path}, cls=CustomJSONEncoder))
            else:
                print(json.dumps({"isSuccess": False, "message": "No data to write to JSON file."}, cls=CustomJSONEncoder))
        else:
            # Execute DML/DDL query and print isSuccess: True
            execute_query(query, is_stored_procedure=is_stored_procedure)
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
        # Read command line arguments
        driver_version = sys.argv[1]
        server_name = sys.argv[2]
        db_name = sys.argv[3]
        sql_file_path = sys.argv[4]

        key = sys.argv[5].lower() == 'true' if len(sys.argv) > 5 else False
        file_path = sys.argv[6] if len(sys.argv) > 6 and sys.argv[6].endswith('.json') else None

        # Set the correct driver based on the input version
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
            print(f"{driver_name} not found")
            sys.exit(1)

        # Read the SQL script from the file
        with open(sql_file_path, 'r') as file:
            query = file.read()

        is_stored_procedure = query.strip().upper().startswith('EXEC')

        if file_path:
            fetch_data_and_write_json(query, file_path, is_stored_procedure=is_stored_procedure, key=key)
        else:
            handle_output(query, key=key, file_path=file_path, is_stored_procedure=is_stored_procedure)

    except Exception as e:
        print(json.dumps({"error": str(e), "isSuccess": False}))
        sys.exit(1)
