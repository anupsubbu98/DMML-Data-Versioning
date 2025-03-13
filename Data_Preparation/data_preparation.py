import os
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler, MinMaxScaler

# Define paths
RAW_DATA_DIR = "Assignment/Raw_data_storage/raw_data/csv/"
OUTPUT_DIR = "Assignment/Data_Preparation"

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Find the latest available CSV file
csv_files = [f for f in os.listdir(RAW_DATA_DIR) if f.endswith(".csv")]

if not csv_files:
    raise FileNotFoundError("No CSV files found in the raw data folder!")

latest_file = max(csv_files, key=lambda f: os.path.getmtime(os.path.join(RAW_DATA_DIR, f)))
latest_file_path = os.path.join(RAW_DATA_DIR, latest_file)

print(f" Using latest file: {latest_file}")

# Load the dataset
df = pd.read_csv(latest_file_path)

# Convert 'TotalCharges' to numeric (fixing string issue)
df["TotalCharges"] = pd.to_numeric(df["TotalCharges"], errors="coerce")

# Data Validation: Check for Missing Values & Duplicates
validation_results = {
    "Missing Values": df.isnull().sum().sum(),
    "Duplicate Rows": df.duplicated().sum()
}
print("Data Validation Results:", validation_results)

# Data Cleaning
df.drop_duplicates(inplace=True)  # Remove duplicate rows
df.fillna(df.select_dtypes(include=[np.number]).mean(), inplace=True)  # Fill numeric missing values with mean

# Drop 'customerID' column if it exists (not useful for modeling)
if "customerID" in df.columns:
    df.drop(columns=["customerID"], inplace=True)

# Standardization & Normalization
scaler = StandardScaler()  # Z-score scaling
df[["MonthlyCharges", "TotalCharges"]] = scaler.fit_transform(df[["MonthlyCharges", "TotalCharges"]])

minmax_scaler = MinMaxScaler()  # Min-Max scaling (0-1)
df[["tenure"]] = minmax_scaler.fit_transform(df[["tenure"]])

# Encoding Categorical Variables
df = pd.get_dummies(df, drop_first=True)

# Exploratory Data Analysis (EDA)
print("Starting EDA...")

# Save Summary Statistics
summary_stats = df.describe()
summary_stats_path = os.path.join(OUTPUT_DIR, "summary_statistics.csv")
summary_stats.to_csv(summary_stats_path)
print(f"Summary statistics saved: {summary_stats_path}")

# Save Histograms
histogram_path = os.path.join(OUTPUT_DIR, "histograms.png")
df.hist(figsize=(12, 8), bins=30)
plt.savefig(histogram_path)
plt.close()
print(f"Histograms saved: {histogram_path}")

# Save Box Plots (for first 5 numerical columns)
boxplot_path = os.path.join(OUTPUT_DIR, "boxplot.png")
plt.figure(figsize=(12, 6))
numeric_cols = df.select_dtypes(include=[np.number]).columns[:5]
sns.boxplot(data=df[numeric_cols])
plt.xticks(rotation=90)
plt.title("Box Plot of Numerical Features")
plt.savefig(boxplot_path)
plt.close()
print(f"Boxplot saved: {boxplot_path}")

# Save Cleaned Data
cleaned_data_path = os.path.join(OUTPUT_DIR, "cleaned_data.csv")
df.to_csv(cleaned_data_path, index=False)
print(f"Data processing completed! Files generated:\n - {cleaned_data_path}\n - {summary_stats_path}\n - {histogram_path}\n - {boxplot_path}")