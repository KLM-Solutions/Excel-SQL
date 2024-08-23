import streamlit as st
import pandas as pd
import psycopg2
from psycopg2 import sql
from datetime import datetime

def sanitize_column_name(name):
    # Replace spaces with underscores and remove any non-alphanumeric characters
    return ''.join(c if c.isalnum() else '_' for c in name).lower()

def excel_to_postgresql(df, table_name):
    sql_statements = []
    
    # Create table statement
    columns = []
    for col, dtype in zip(df.columns, df.dtypes):
        sanitized_col = sanitize_column_name(col)
        if dtype == 'object':
            columns.append(f"\"{sanitized_col}\" TEXT")
        elif dtype in ['int64', 'int32']:
            columns.append(f"\"{sanitized_col}\" INTEGER")
        elif dtype in ['float64', 'float32']:
            columns.append(f"\"{sanitized_col}\" NUMERIC")
        elif dtype in ['datetime64[ns]', 'datetime64']:
            columns.append(f"\"{sanitized_col}\" TIMESTAMP")
        else:
            columns.append(f"\"{sanitized_col}\" TEXT")  # Default to TEXT for unknown types
    
    create_table = f"CREATE TABLE IF NOT EXISTS \"{table_name}\" (\n    " + ",\n    ".join(columns) + "\n);"
    sql_statements.append(create_table)
    
    # Insert statements
    for _, row in df.iterrows():
        values = []
        for value in row:
            if pd.isna(value):
                values.append("NULL")
            elif isinstance(value, str):
                values.append(f"'{value.replace('\'', '\'\'')}'")
            elif isinstance(value, (int, float)):
                values.append(str(value))
            elif isinstance(value, datetime):
                values.append(f"'{value.isoformat()}'")
            else:
                values.append(f"'{str(value)}'")
        
        sanitized_columns = ", ".join(f"\"{sanitize_column_name(col)}\"" for col in df.columns)
        values_str = ", ".join(values)
        sql = f"INSERT INTO \"{table_name}\" ({sanitized_columns}) VALUES ({values_str});"
        sql_statements.append(sql)
    
    return sql_statements

def create_database_and_execute_sql(host, user, password, db_name, sql_statements):
    # Connect to default 'postgres' database to create a new database
    conn = psycopg2.connect(
        host=host,
        database="postgres",
        user=user,
        password=password
    )
    conn.autocommit = True
    cur = conn.cursor()
    
    # Create new database
    cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
    
    # Close connection to 'postgres' database
    cur.close()
    conn.close()
    
    # Connect to the newly created database
    conn = psycopg2.connect(
        host=host,
        database=db_name,
        user=user,
        password=password
    )
    conn.autocommit = True
    cur = conn.cursor()
    
    # Execute SQL statements
    for statement in sql_statements:
        cur.execute(statement)
    
    # Close connection
    cur.close()
    conn.close()

st.title('Excel to PostgreSQL Converter')

uploaded_file = st.file_uploader("Choose an Excel file", type="xlsx")

if uploaded_file is not None:
    st.success("File successfully uploaded!")
    
    # Read Excel file
    df = pd.read_excel(uploaded_file)
    
    st.write("Preview of the Excel data:")
    st.dataframe(df.head())
    
    # Database connection details
    st.subheader("Database Connection Details")
    host = st.text_input("Host", "localhost")
    user = st.text_input("Username")
    password = st.text_input("Password", type="password")
    db_name = st.text_input("Enter the name for your PostgreSQL database:", "your_database_name")
    table_name = st.text_input("Enter the name for your PostgreSQL table:", "your_table_name")
    
    if st.button('Convert and Insert into PostgreSQL'):
        if not user or not password:
            st.error("Please provide both username and password.")
        else:
            sql_statements = excel_to_postgresql(df, table_name)
            
            try:
                create_database_and_execute_sql(host, user, password, db_name, sql_statements)
                st.success(f"Data successfully inserted into {db_name}.{table_name}")
            except Exception as e:
                st.error(f"An error occurred: {str(e)}")

st.markdown("---")
st.write("Created with Streamlit and Python for PostgreSQL")
