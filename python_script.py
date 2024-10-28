# # import pyodbc
# # import json
# # import os
# # import sys
# # import decimal
# # from datetime import date, datetime

# # # Configuration for database connection
# # config = {
# #     "driver": "ODBC Driver 17 for SQL Server",
# #     "trusted_connection": "yes",
# #     "trustServerCertificate": "yes"
# # }

# # pool = None

# # def check_odbc_driver(driver_name):
# #     """ Check if the specified ODBC driver is installed """
# #     installed_drivers = pyodbc.drivers()
# #     return driver_name in installed_drivers

# # def get_connection_string(config):
# #     """ Generate a connection string from the config """
# #     return (
# #         f"Driver={{{config['driver']}}};"
# #         f"Server={config['server']};"
# #         f"Database={config['database']};"
# #         "Trusted_Connection=yes;"
# #         "TrustServerCertificate=yes"
# #     )

# # def connect_to_database():
# #     """ Establish a connection to the database """
# #     global pool
# #     try:
# #         if not pool:
# #             connection_string = get_connection_string(config)
# #             pool = pyodbc.connect(connection_string)
# #         return pool
# #     except Exception as err:
# #         raise err

# # def get_connection():
# #     """ Get or switch the connection based on the database name """
# #     global pool
# #     try:
# #         if not pool:
# #             connect_to_database()
# #         if pool:
# #             pool.close()
# #         pool = pyodbc.connect(get_connection_string(config))

# #         return pool
# #     except Exception as err:
# #         raise err

# # # def fetch_data_and_handle_output(query, file_path=None, batch_size=1000, is_stored_procedure=False, key=False):
# # #     """ Fetch data, print it if key=True, and handle DDL/DML queries or stored procedures """
# # #     connection = get_connection()
# # #     try:
# # #         cursor = connection.cursor()

# # #         # Print the query if key is True
# # #         if key:
# # #             print(f"Executing query: {query}")

# # #         # Execute the query (can be SELECT, DDL, DML, or a stored procedure)
# # #         cursor.execute(query)

# # #         # Check if the query returns results (e.g., SELECT queries)
# # #         if cursor.description:
# # #             results = []
# # #             columns = [column[0] for column in cursor.description]

# # #             while True:
# # #                 rows = cursor.fetchmany(batch_size)
# # #                 if not rows:
# # #                     break
# # #                 for row in rows:
# # #                     results.append(dict(zip(columns, row)))

# # #             # If key is True, print the results
# # #             if key:
# # #                 print(f"{json.dumps({'isSuccess': True, 'results': {json.dumps(results, cls=CustomJSONEncoder)}})}")
# # #             else:
# # #                 if file_path:
# # #                     with open(file_path, mode='w', encoding='utf-8') as file:
# # #                         json.dump(results, file, cls=CustomJSONEncoder)
# # #                     print(json.dumps({"isSuccess": True,"results": file_path}))
# # #         else:
# # #             # This handles DDL/DML queries (e.g., INSERT, UPDATE, DELETE, CREATE)
# # #             connection.commit()  # Ensure DML queries are committed
# # #             if key:
# # #                 print("Query executed successfully, no result set returned.")
# # #             else:
# # #                 if file_path:
# # #                     with open(file_path, mode='w', encoding='utf-8') as file:
# # #                         json.dump({"message": "Query executed successfully, no result set returned."}, file)
# # #                     print(f"Query executed successfully, no result set returned.")

# # #         # Print success flag if everything goes well
# # #         if not file_path:
# # #             print(json.dumps({"isSuccess": True}))
# # #     except Exception as err:
# # #         # If there's an error, return failure
# # #         print(json.dumps({"isSuccess": False, "error": str(err)}))
# # #         raise err
# # #     finally:
# # #         cursor.close()
# # #         connection.close()

# # def fetch_data_and_handle_output(query, file_path=None, batch_size=1000, is_stored_procedure=False, key=False):
# #     """ Fetch data, print it if key=True, and handle DDL/DML queries or stored procedures """
# #     connection = get_connection()
# #     try:
# #         cursor = connection.cursor()

# #         # Print the query if key is True
# #         if key:
# #             print(f"Executing query: {query}")

# #         # Execute the query (can be SELECT, DDL, DML, or a stored procedure)
# #         cursor.execute(query)

# #         # If the query is DML/DDL and no result is expected
# #         connection.commit()
# #         print(cursor.description)
# #         # Check if the query returns results (e.g., SELECT queries)
# #         if cursor.description:
# #             results = []
# #             columns = [column[0] for column in cursor.description]

# #             while True:
# #                 rows = cursor.fetchmany(batch_size)
# #                 if not rows:
# #                     break
# #                 for row in rows:
# #                     results.append(dict(zip(columns, row)))

# #             # If key is True, print the results
# #             if key:
# #                 print(f"{json.dumps({'isSuccess': True, 'results': {json.dumps(results, cls=CustomJSONEncoder)}})}")
# #             else:
# #                 if file_path:
# #                     with open(file_path, mode='w', encoding='utf-8') as file:
# #                         json.dump(results, file, cls=CustomJSONEncoder)
# #                     print(json.dumps({"isSuccess": True, "results": file_path}))  # Return file path on success
# #         else:
# #             # If no results are returned and only DML/DDL is executed, commit changes
# #             if file_path:
# #                 with open(file_path, mode='w', encoding='utf-8') as file:
# #                     json.dump({"message": "Query executed successfully, no result set returned."}, file)
# #                 print(json.dumps({"isSuccess": True, "results": file_path}))

# #     except Exception as err:
# #         # If there's an error, return failure
# #         print(json.dumps({"isSuccess": False, "error": str(err)}))
# #         raise err
# #     finally:
# #         cursor.close()
# #         connection.close()


# # class CustomJSONEncoder(json.JSONEncoder):
# #     def default(self, obj):
# #         if isinstance(obj, decimal.Decimal):
# #             return float(obj) 
# #         if isinstance(obj, (date, datetime)):
# #             return obj.isoformat()  
# #         return super(CustomJSONEncoder, self).default(obj)


# # if __name__ == "__main__":  
# #     try:
# #         # Read command line arguments
# #         server_name = sys.argv[1]
# #         db_name = sys.argv[2]
# #         query = sys.argv[3]

# #         # Check if the key argument is passed, default to False if not
# #         key = sys.argv[4].lower() == 'true' if len(sys.argv) > 4 else False

# #         # If key is False, ensure a JSON file path is provided or handle without it
# #         file_path = None
# #         if not key:
# #             if len(sys.argv) > 5:
# #                 file_path = sys.argv[5]
# #                 if file_path and not file_path.endswith('.json'):
# #                     print(json.dumps({"error": "File path must have .json extension when key is False"}))
# #                     sys.exit(1)

# #         config['server'] = server_name
# #         config['database'] = db_name
        
# #         driver_name = config['driver']
# #         if not check_odbc_driver(driver_name):
# #             print(f"{driver_name} not found")
# #             sys.exit(1)

# #         # Determine if the query is a stored procedure or DML/DDL
# #         is_stored_procedure = query.strip().upper().startswith('EXEC')

# #         # Call the function to fetch data or execute DDL/DML and either print or write to a JSON file
# #         fetch_data_and_handle_output(query, file_path, is_stored_procedure=is_stored_procedure, key=key)
# #     except Exception as e:
# #         print(json.dumps({"error": str(e), "isSuccess": False}))

import pyodbc
import json
import os
import sys
import decimal
from datetime import date, datetime

# Configuration for database connection
config = {
    "driver": "ODBC Driver 17 for SQL Server",
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
    return (
        f"Driver={{{config['driver']}}};"
        f"Server={config['server']};"
        f"Database={config['database']};"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes"
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
    """ Get or switch the connection based on the database name """
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

def fetch_data_and_write_json(query, file_path, batch_size=1000, is_stored_procedure=False, key=False):
    """ Fetch data and write to JSON, handling stored procedures if necessary """
    connection = get_connection()
    
    try:
        cursor = connection.cursor()

        if is_stored_procedure:
            cursor.execute(query)  # Execute the stored procedure
        else:
            cursor.execute(query)  # Execute the normal query

        json_filename = None
        if file_path is not None:
            json_filename = f"{file_path}"
            if os.path.exists(json_filename):
                os.remove(json_filename)

        results = []
        has_data = False  # Flag to detect if any data was fetched

        # Loop through result sets until we find data
        while True:
            columns = cursor.description  # Check for result set description
            if columns:  # If there's a result set
                while True:
                    rows = cursor.fetchmany(batch_size)
                    if rows:
                        has_data = True  # We found data
                        columns = [column[0] for column in cursor.description]
                        for row in rows:
                            results.append(dict(zip(columns, row)))
                    else:
                        break  # No more rows in this result set

            # Check for more result sets
            if not cursor.nextset():
                break  # No more result set

        # Write the results to the JSON file
        if has_data and key is False and file_path:
            with open(json_filename, mode='w', encoding='utf-8') as file:
                json.dump(results, file, cls=CustomJSONEncoder)
            print(json.dumps({"isSuccess": True, "results": file_path}))
        elif key is True and has_data:
            print(json.dumps({"isSuccess": True, "results": results}))
        else:
            print(json.dumps({"isSuccess": True, "results": []}))

        return json_filename
    except Exception as err:
        connection.rollback()  # Rollback the transaction if any error occurs
        raise err
    finally:
        cursor.close()
        connection.close()

def fetch_data_and_handle_output(query, file_path=None, batch_size=1000, is_stored_procedure=False, key=False):
    """ Fetch data, print it if key=True, and handle DDL/DML queries or stored procedures """
    connection = get_connection()
    try:
        cursor = connection.cursor()

        # Print the query if key is True
        if key:
            print(f"Executing query: {query}")

        # Execute the query (can be SELECT, DDL, DML, or a stored procedure)
        cursor.execute(query)

        # Commit the transaction for DML/DDL queries
        connection.commit()

        # Check if the query returns results (e.g., SELECT queries)
        if cursor.description:
            results = []
            columns = [column[0] for column in cursor.description]

            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                for row in rows:
                    results.append(dict(zip(columns, row)))

            # If key is True, print the results
            if key:
                print(json.dumps({"isSuccess": True, "results": results}))
            else:
                if file_path:
                    with open(file_path, mode='w', encoding='utf-8') as file:
                        json.dump(results, file, cls=CustomJSONEncoder)
                    print(json.dumps({"isSuccess": True, "results": file_path}))  # Return file path on success
        else:
            # If no results are returned and only DML/DDL is executed, commit changes
            if file_path:
                with open(file_path, mode='w', encoding='utf-8') as file:
                    json.dump({"message": "Query executed successfully, no result set returned."}, file)
                print(json.dumps({"isSuccess": True, "results": file_path}))
            else:
                print(json.dumps({"isSuccess": True, "results": []}))

    except Exception as err:
        print(json.dumps({"isSuccess": False, "error": str(err)}))
        raise err
    finally:
        cursor.close()
        connection.close()

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
        server_name = sys.argv[1]
        db_name = sys.argv[2]
        query = sys.argv[3]

        # Check if the key argument is passed, default to False if not
        key = sys.argv[4].lower() == 'true' if len(sys.argv) > 4 else False

        # If key is False, ensure a JSON file path is provided or handle without it
        file_path = None
        
        if sys.argv[5] and sys.argv[5].endswith('.json'):
            file_path = sys.argv[5]

        config['server'] = server_name
        config['database'] = db_name

        driver_name = config['driver']
        if not check_odbc_driver(driver_name):
            print(f"{driver_name} not found")
            sys.exit(1)

        # Determine if the query is a stored procedure or DML/DDL
        is_stored_procedure = query.strip().upper().startswith('EXEC')

        # Call the function to fetch data or execute DDL/DML and either print or write to a JSON file
        if file_path:
            fetch_data_and_write_json(query, file_path, is_stored_procedure=is_stored_procedure, key=key)
        else:
            fetch_data_and_handle_output(query, file_path, is_stored_procedure=is_stored_procedure, key=key)
    except Exception as e:
        print(json.dumps({"error": str(e), "isSuccess": False}))
        sys.exit(1)

