import os
import pandas as pd
import logging
import openml
from datetime import datetime

# Define directories
LOGS_DIR = "Assignment/Raw_data_storage/logs"
os.makedirs(LOGS_DIR, exist_ok=True)

# Configure logging with a dated filename
today_date = datetime.today().strftime("%Y-%m-%d")
LOG_FILE = os.path.join(LOGS_DIR, f"data_ingestion_{today_date}.log")
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def load_csv_data(file_path):
    """Loads a CSV file into a pandas DataFrame."""
    try:
        df = pd.read_csv(file_path)
        logging.info(f"Successfully loaded CSV data from {file_path}")
        return df
    except Exception as e:
        logging.error(f"Error loading CSV data: {str(e)}")
        return None


def fetch_openml_data(dataset_id):
    """Fetches data from OpenML and converts it to a DataFrame."""
    try:
        dataset = openml.datasets.get_dataset(dataset_id)
        X, y, _, _ = dataset.get_data(target=dataset.default_target_attribute)
        df = pd.concat([X, y], axis=1)
        logging.info(f"Successfully fetched dataset {dataset_id} from OpenML")
        return df
    except Exception as e:
        logging.error(f"Error fetching OpenML data: {str(e)}")
        return None


def main():
    """Main function to fetch and return data."""
    csv_file_path = "customer_churn_data.csv"
    openml_dataset_id = 42178

    # Load CSV Data
    customer_df = load_csv_data(csv_file_path)

    # Fetch OpenML API Data
    transaction_df = fetch_openml_data(openml_dataset_id)

    return customer_df, transaction_df


if __name__ == "__main__":
    customer_df, transaction_df = main()
