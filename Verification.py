import psycopg2
from pyhive import presto
import pyodbc
import configparser
import os
import csv
from datetime import datetime, timedelta
import logging

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

def query_gpdb(config, product_types, start_ip, query_start_time):
    total_received_counts = {product_type: 0 for product_type in product_types}
    try:
        conn = psycopg2.connect(
            user=config["gpdb"]["gpdb_user_name"],
            password=config["gpdb"]["gpdb_pass"],
            host=config["gpdb"]["gpdb_host"],
            port=config["gpdb"]["gpdb_port"],
            database=config["gpdb"]["gpdb_database"]
        )
        cursor = conn.cursor()
        for product_type in product_types:
            query = (f"SELECT count(distinct product_id) as amount "
                     f"FROM merit.dbo.product_participants "
                     f"WHERE ip_address::text LIKE '{start_ip}%' "
                     f"AND product_start_time > '{query_start_time}' "
                     f"AND site_number = 1 "
                     f"AND product_type = {product_type};")
            logging.info(f"Executing GPDB query: {query}")
            cursor.execute(query)
            record = cursor.fetchone()
            total_received_counts[product_type] += record[0] if record else 0
            logging.info(f"GPDB query result for product type {product_type}: {record[0] if record else 0}")
    except Exception as e:
        logging.error(f"GPDB Error: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()
    return total_received_counts

def query_presto(config, product_types, start_ip, injection_start_time):
    total_received_counts = {product_type: 0 for product_type in product_types}
    try:
        conn = presto.connect(
            username=config["presto"]["presto_user_name"],
            host=config["presto"]["presto_host"],
            port=config["presto"]["presto_port"],
            catalog=config["presto"]["presto_catalog"],
            schema=config["presto"]["presto_schema"]
        )
        cursor = conn.cursor()
        last_date_to_check = injection_start_time + timedelta(days=1)
        tables = []
        temp_time = injection_start_time
        while temp_time < last_date_to_check:
            temp_time_str = temp_time.strftime("%Y_%m_%d")
            query = (f"SELECT table_name FROM information_schema.tables "
                     f"WHERE table_name LIKE '%xdr%' AND table_name NOT LIKE '%index%' AND table_name LIKE '%{temp_time_str}%'")
            cursor.execute(query)
            tables.extend(cursor.fetchall())
            temp_time += timedelta(days=1)

        if not tables:
            logging.warning("No tables found in Presto (Hive) for the given date range.")

        for product_type in product_types:
            amount = 0
            for table in tables:
                if not table:
                    continue
                query = (f"SELECT count(distinct product_id) as amount "
                         f"FROM {table[0]} "
                         f"WHERE product_type = {product_type} "
                         f"AND site_id = 1 "
                         f"AND identifiers LIKE '%{start_ip}%'")
                logging.info(f"Executing Presto (Hive) query: {query}")
                cursor.execute(query)
                records = cursor.fetchone()
                amount += records[0] if records else 0
                logging.info(f"Presto (Hive) query result for table {table[0]} and product type {product_type}: {records[0] if records else 0}")
            total_received_counts[product_type] += amount
    except Exception as e:
        logging.error(f"Presto (Hive) Error: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()
    return total_received_counts

def query_ase(config, product_types, start_ip, query_start_time):
    total_received_counts = {product_type: 0 for product_type in product_types}
    conn = None  # Initialize conn to None
    try:
        conn_str = (f"DRIVER={{FreeTDS}};"  # Update this part with the correct driver name
                    f"SERVER={config['ase']['ase_host']},{config['ase']['ase_port']};"

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

    # Initialize dictionaries to store counts
    gpdb_product_types = config["gpdb"]["gpdb_enums"].split(',') if 'gpdb' in config else []
    presto_product_types = config["presto"]["presto_enums"].split(',') if 'presto' in config else []
    ase_product_types = config["ase"]["ase_enums"].split(',') if 'ase' in config else []
    all_product_types = list(set(gpdb_product_types + presto_product_types + ase_product_types))

    # Get start IP from config data
    start_ip = config["injection"]["start_ip"]

    # Time variables
    injection_start_time = datetime.strptime(config["injection"]["injection_start_time"], '%Y-%m-%d %H:%M:%S')
    query_start_time = injection_start_time - timedelta(minutes=200)  # 3 hours and 20 minutes

    # Query GPDB
    gpdb_counts = query_gpdb(config, gpdb_product_types, start_ip, query_start_time) if gpdb_product_types else {}

    # Query Presto (Hive)
    presto_counts = query_presto(config, presto_product_types, start_ip, injection_start_time) if presto_product_types else {}

    # Query ASE (Sybase)
    ase_counts = query_ase(config, ase_product_types, start_ip, query_start_time) if ase_product_types else {}

    # Merge counts
    total_received_counts = {product_type: (gpdb_counts.get(product_type, 0) +
                                            presto_counts.get(product_type, 0) +
                                            ase_counts.get(product_type, 0))
                             for product_type in all_product_types}

    # Output results to CSV
    output_file_name = os.path.join(current_dir, 'Stability_results.csv')
    audit_results(all_product_types, total_received_counts, output_file_name)

    logging.info(f"Script completed. Results have been saved to {output_file_name}.")

if __name__ == "__main__":
    main()
