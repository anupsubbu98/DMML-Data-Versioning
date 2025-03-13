import pandas as pd
from Assignment.Data_Transformation.connection import SnowflakeConnection

# Initialize Snowflake Connection
conn = SnowflakeConnection()

# Step 1: Create Metadata Table
create_table_query = """
CREATE TABLE IF NOT EXISTS FEATURE_METADATA (
    feature_name STRING PRIMARY KEY,
    data_type STRING,
    description STRING
)
"""
conn.execute_query(create_table_query)

# Step 2: Insert Metadata
insert_metadata_query = """
INSERT INTO FEATURE_METADATA (feature_name, data_type, description) VALUES
('gender', 'INT64', 'Encoded gender (e.g., Male=0, Female=1)'),
('SeniorCitizen', 'INT64', 'Indicates if the customer is a senior citizen (1 = Yes, 0 = No)'),
('Partner', 'INT64', 'Indicates if the customer has a partner (1 = Yes, 0 = No)'),
('Dependents', 'INT64', 'Indicates if the customer has dependents (1 = Yes, 0 = No)'),
('tenure', 'FLOAT32', 'Number of months the customer has been with the company'),
('PhoneService', 'INT64', 'Indicates if the customer has phone service (1 = Yes, 0 = No)'),
('PaperlessBilling', 'INT64', 'Indicates if the customer uses paperless billing (1 = Yes, 0 = No)'),
('MonthlyCharges', 'FLOAT32', 'Monthly charges billed to the customer'),
('TotalCharges', 'FLOAT32', 'Total amount charged to the customer'),
('Churn', 'INT64', 'Indicates if the customer has churned (1 = Yes, 0 = No)'),
('AvgMonthlySpend', 'FLOAT32', 'Average monthly spending of the customer'),
('TenureCategory', 'INT64', 'Binned tenure category (encoded)'),
('ActivityLevel', 'INT64', 'Customer activity level based on MonthlyCharges (encoded)'),
('MultipleLines_No phone service', 'INT64', 'Indicates if the customer has no phone service'),
('MultipleLines_Yes', 'INT64', 'Indicates if the customer has multiple lines'),
('InternetService_Fiber optic', 'INT64', 'Indicates if the customer has fiber optic internet service'),
('InternetService_No', 'INT64', 'Indicates if the customer has no internet service'),
('OnlineSecurity_No internet service', 'INT64', 'Indicates if the customer has no internet security service'),
('OnlineSecurity_Yes', 'INT64', 'Indicates if the customer has online security service'),
('OnlineBackup_No internet service', 'INT64', 'Indicates if the customer has no online backup service'),
('OnlineBackup_Yes', 'INT64', 'Indicates if the customer has online backup service'),
('DeviceProtection_No internet service', 'INT64', 'Indicates if the customer has no device protection service'),
('DeviceProtection_Yes', 'INT64', 'Indicates if the customer has device protection service'),
('TechSupport_No internet service', 'INT64', 'Indicates if the customer has no tech support service'),
('TechSupport_Yes', 'INT64', 'Indicates if the customer has tech support service'),
('StreamingTV_No internet service', 'INT64', 'Indicates if the customer has no streaming TV service'),
('StreamingTV_Yes', 'INT64', 'Indicates if the customer has streaming TV service'),
('StreamingMovies_No internet service', 'INT64', 'Indicates if the customer has no streaming movies service'),
('StreamingMovies_Yes', 'INT64', 'Indicates if the customer has streaming movies service'),
('Contract_One year', 'INT64', 'Indicates if the customer has a one-year contract'),
('Contract_Two year', 'INT64', 'Indicates if the customer has a two-year contract'),
('PaymentMethod_Credit card (automatic)', 'INT64', 'Indicates if the customer pays by automatic credit card payments'),
('PaymentMethod_Electronic check', 'INT64', 'Indicates if the customer pays by electronic check'),
('PaymentMethod_Mailed check', 'INT64', 'Indicates if the customer pays by mailed check')
"""

conn.execute_query(insert_metadata_query)

# Step 3: Fetch Data
fetch_query = """SELECT * FROM CUSTOMER_CHURN.PUBLIC.FEATURE_METADATA LIMIT 100"""
data = conn.execute_query(fetch_query)

# Convert to DataFrame
df = pd.DataFrame(data)
print(df)

# Commit and close connection
conn.commit()
conn.close()