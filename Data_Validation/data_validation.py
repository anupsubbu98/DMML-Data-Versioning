import os
import pandas as pd
import matplotlib.pyplot as plt
from fpdf import FPDF
from scipy.stats import zscore

# Define input and output paths
RAW_DATA_DIR = "Assignment/Raw_data_storage/raw_data/csv/"
OUTPUT_FOLDER = "Assignment/Data_Validation"

# Ensure the output directory exists
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Find the latest CSV file in the raw data folder
csv_files = [f for f in os.listdir(RAW_DATA_DIR) if f.endswith(".csv")]

if not csv_files:
    raise FileNotFoundError("No CSV files found in the raw data folder!")

latest_file = max(csv_files, key=lambda f: os.path.getmtime(os.path.join(RAW_DATA_DIR, f)))
latest_file_path = os.path.join(RAW_DATA_DIR, latest_file)

print(f" Using latest file: {latest_file}")

# Load the dataset
df = pd.read_csv(latest_file_path)

print(df.head).__format__

# Convert 'TotalCharges' to numeric
df['TotalCharges'] = pd.to_numeric(df['TotalCharges'], errors='coerce')

# Dictionary to store validation results
validation_results = {}

# 1. Check for Missing or Empty Values
validation_results['Missing Values'] = df.isnull().sum().sum()

# 2. Check for Duplicate Rows
validation_results['Duplicate Rows'] = df.duplicated().sum()

# 3. Validate Data Types
validation_results['Invalid Tenure'] = df[(df['tenure'] < 0) | (df['tenure'] > 100)].shape[0]
validation_results['Invalid Monthly Charges'] = df[df['MonthlyCharges'] < 0].shape[0]
validation_results['Invalid Total Charges'] = df[df['TotalCharges'] < 0].shape[0]

# 4. Identify Anomalies using Z-score (for Monthly Charges)
df['MonthlyCharges_Zscore'] = zscore(df['MonthlyCharges'])
df['Anomaly'] = df['MonthlyCharges_Zscore'].abs() > 3  # Mark anomalies
validation_results['Anomalies in Monthly Charges'] = df['Anomaly'].sum()

# 5. Save Validation Report to CSV
csv_report_path = os.path.join(OUTPUT_FOLDER, "data_validation_report.csv")
validation_report_df = pd.DataFrame(list(validation_results.items()), columns=['Check', 'Count'])
validation_report_df.to_csv(csv_report_path, index=False)

# 6. Generate PDF Report
class PDF(FPDF):
    def header(self):
        self.set_font("Arial", "B", 14)
        self.cell(200, 10, "Data Validation Report", ln=True, align="C")

pdf = PDF()
pdf.set_auto_page_break(auto=True, margin=15)
pdf.add_page()
pdf.set_font("Arial", size=12)

# Add validation results to PDF
for key, value in validation_results.items():
    pdf.cell(0, 10, f"{key}: {value}", ln=True)

# Save PDF
pdf_report_path = os.path.join(OUTPUT_FOLDER, "data_validation_report.pdf")
pdf.output(pdf_report_path)

print(f" Data validation completed!\nLatest File: {latest_file}\nCSV Report: {csv_report_path}\nPDF Report: {pdf_report_path}")