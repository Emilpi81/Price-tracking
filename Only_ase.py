import pymssql
import configparser
import os
import csv
from datetime import datetime, timedelta
import logging
import pyodbc

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to audit results and write to CSV
def audit_results(product_types, total_received_counts, file_name):
    with open(file_name, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Product Type", "Total Received"])
        for product_type in product_types:
            total_received = total_received_counts.get(product_type, 0)
            writer.writerow([product_type, total_received])

def read_config(config_file_path):
    config = configparser.ConfigParser()
    try:
        config.read(config_file_path)
        if not config.sections():
            raise ValueError("No sections found in the config file.")
    except Exception as e:
        logging.error(f"Error reading config file: {e}")
        exit(1)
    logging.debug(f"Config sections: {config.sections()}")
    return config

def query_ase(config, product_types, start_ip, query_start_time):
    total_received_counts = {product_type: 0 for product_type in product_types}
    conn = None  # Initialize conn to None
    try:
        conn_str = (f"DSN=ASEDataSource;"  # Use the DSN you created
                    f"UID={config['ase']['ase_user_name']};"
                    f"PWD={config['ase']['ase_pass']}")
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        for product_type in product_types:
            query = (f"SELECT COUNT(DISTINCT pp.product_id) AS amount "
                     f"FROM products.dbo.PRODUCT_PARTICIPANTS pp "
                     f"JOIN products.dbo.PRODUCTS p ON pp.product_id = p.product_id "
                     f"WHERE pp.participant_identifier LIKE '{start_ip}%' "
                     f"AND p.product_type = {product_type} "
                     f"AND p.product_start_time > '{query_start_time}';")
            logging.info(f"Executing ASE query: {query}")
            cursor.execute(query)
            record = cursor.fetchone()
            total_received_counts[product_type] += record[0] if record else 0
            logging.info(f"ASE query result for product type {product_type}: {record[0] if record else 0}")
    except KeyError as e:
        logging.error(f"Missing config key: {e}")
    except Exception as e:
        logging.error(f"ASE Error: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()
    return total_received_counts

def main():
    # Paths
    current_dir = os.getcwd()
    config_file_path = os.path.join(current_dir, 'config.ini')

    # Verify the existence of the config file
    if not os.path.exists(config_file_path):
        logging.error(f"Config file not found at path: {config_file_path}")
        exit(1)

    # Read config file
    config = read_config(config_file_path)

    # Define product types and other variables from config
    product_types = config['ase']['ase_enums'].split(',')
    start_ip = config['injection']['start_ip']
    query_start_time = config['injection']['injection_start_time']

    # Query ASE and get results
    total_received_counts = query_ase(config, product_types, start_ip, query_start_time)

    # Define output file name
    output_file_name = os.path.join(current_dir, 'audit_results.csv')

    # Audit results and write to CSV
    audit_results(product_types, total_received_counts, output_file_name)

    logging.info(f"Audit results written to {output_file_name}")

if __name__ == "__main__":
    main()
