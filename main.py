import pandas as pd
import psycopg2
from datetime import datetime

# Extract: Load data from CSV
def extract_data(file_path):
    return pd.read_csv(file_path)

# Transform: Clean and filter data
def transform_data(df):
    # Remove rows with missing critical fields
    df.dropna(subset=['age', 'email'], inplace=True)
    
    # Filter rows where age < 18
    df = df[df['age'] >= 18]
    
    # Ensure purchase_amount is numeric, replace NaN with 0
    df['purchase_amount'] = pd.to_numeric(df['purchase_amount'], errors='coerce').fillna(0.00)
    
    # Convert 'join_date' to datetime
    df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')
    
    return df

# Load: Insert data into PostgreSQL
def load_data(df, conn):
    cursor = conn.cursor()
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO users (id, name, age, country, email, join_date, purchase_amount, active) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (row['id'], row['name'], row['age'], row['country'], row['email'], row['join_date'], row['purchase_amount'], row['active']))
    conn.commit()

# Main ETL function
def etl_pipeline(file_path, conn):
    df = extract_data(file_path)
    df_transformed = transform_data(df)
    load_data(df_transformed, conn)

# PostgreSQL connection setup
conn = psycopg2.connect(
    host="localhost",
    database="etl_db",
    user="user",
    password="password"
)

# Run ETL
etl_pipeline('data.csv', conn)
conn.close()
