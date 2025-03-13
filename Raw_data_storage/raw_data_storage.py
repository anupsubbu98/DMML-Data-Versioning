import os
import sys
import pandas as pd
import logging
from datetime import datetime
from fpdf import FPDF

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from Data_Ingestion.data_ingestions import main

# Define directories inside Assignment/Raw_data_storage
BASE_DIR = "Assignment/Raw_data_storage"
RAW_DATA_DIR = os.path.join(BASE_DIR, "raw_data")
CSV_DIR = os.path.join(RAW_DATA_DIR, "csv")
API_DIR = os.path.join(RAW_DATA_DIR, "api")
LOGS_DIR = os.path.join(BASE_DIR, "logs")

# Ensure directories exist
os.makedirs(CSV_DIR, exist_ok=True)
os.makedirs(API_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

# Configure logging
today_date = datetime.today().strftime("%Y-%m-%d")
LOG_FILE = os.path.join(LOGS_DIR, f"data_storage_{today_date}.log")
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def save_raw_data(df, file_name, folder):
    """Saves DataFrame as a CSV in the specified folder."""
    try:
        file_path = os.path.join(folder, file_name)
        df.to_csv(file_path, index=False)
        logging.info(f"Successfully saved raw data to {file_path}")
    except Exception as e:
        logging.error(f"Error saving raw data: {str(e)}")


def main_storage():
    """Handles saving ingested data to structured storage."""
    customer_df, transaction_df = main()  # Fetch data from ingestion script

    if customer_df is not None:
        save_raw_data(customer_df, f"csv_data_{today_date}.csv", CSV_DIR)

    if transaction_df is not None:
        save_raw_data(transaction_df, f"api_data_{today_date}.csv", API_DIR)

    # Generate folder structure PDF
    generate_folder_structure_pdf(BASE_DIR)


def get_folder_structure(root_dir, indent=0):
    """Recursively retrieves folder structure as a string (without emojis for FPDF)."""
    structure = ""
    for item in sorted(os.listdir(root_dir)):
        path = os.path.join(root_dir, item)
        if os.path.isdir(path):
            structure += "   " * indent + f"[Folder] {item}\n"
            structure += get_folder_structure(path, indent + 1)
        else:
            structure += "   " * indent + f"- {item}\n"
    return structure



def generate_folder_structure_pdf(root_directory):
    """Creates a PDF file displaying the folder structure."""
    folder_structure_text = get_folder_structure(root_directory)

    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Courier", size=10)

    # Title
    pdf.set_font("Arial", style="B", size=16)
    pdf.cell(200, 10, "Folder Structure - Raw Data Storage", ln=True, align="C")
    pdf.ln(10)

    # Add folder structure to PDF
    pdf.set_font("Courier", size=10)
    pdf.multi_cell(0, 5, folder_structure_text)

    pdf_filename = os.path.join(BASE_DIR, f"folder_structure_{today_date}.pdf")
    pdf.output(pdf_filename)

    print(f"PDF generated successfully: {pdf_filename}")


if __name__ == "__main__":
    main_storage()
