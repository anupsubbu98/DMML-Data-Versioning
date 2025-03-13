import pandas as pd
# from Assignment.Data_Transformation.connection import SnowflakeConnection

def get_all_features():
    """Retrieve all feature records from the feature store."""
    conn = SnowflakeConnection()
    
    query = """
    WITH LatestFeatures AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY "customerID" ORDER BY FEATURE_VERSION DESC) AS rn
        FROM CUSTOMER_CHURN.PUBLIC.FEATURE_STORE_TABLE
    )
    SELECT * FROM LatestFeatures
    WHERE rn = 1;  -- Fetch only the latest feature version per customer
    """

    df = conn.execute_query(query)
    conn.close()
    
    return df

# Example: Retrieve all customer features
customer_features = get_all_features()
customer_features
