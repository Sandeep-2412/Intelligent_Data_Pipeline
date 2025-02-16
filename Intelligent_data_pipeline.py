import os
import pandas as pd
import numpy as np
import xml.etree.ElementTree as ET
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine, text
import urllib.parse

data_dir = "C:/Users/sande/OneDrive/Desktop/ETL/data_samples"
os.makedirs(data_dir, exist_ok=True)

formats = {
    "csv": "data.csv",
    "json": "data.json",
    "xml": "data.xml",
    "excel": "data.xlsx",
    "parquet": "data.parquet",
    "sql": "data.db",
    "txt": "data.txt",
    "log": "data.log",
    "yaml": "data.yaml",
    "hdf5": "data.h5"
}

def generate_sample_data():
    df = pd.DataFrame({
        "ID": range(1, 101),
        "Value": np.random.rand(100) * 100,
        "Category": np.random.choice(['A', 'B', 'C'], 100)
    })
    df.to_csv(os.path.join(data_dir, formats["csv"]), index=False)
    df.to_json(os.path.join(data_dir, formats["json"]), orient="records")
    df.to_excel(os.path.join(data_dir, formats["excel"]), index=False)
    df.to_parquet(os.path.join(data_dir, formats["parquet"]))
    df.to_hdf(os.path.join(data_dir, formats["hdf5"]), key="df", mode="w")
    root = ET.Element("data")
    for _, row in df.iterrows():
        item = ET.SubElement(root, "record")
        for col in df.columns:
            ET.SubElement(item, col).text = str(row[col])
    tree = ET.ElementTree(root)
    tree.write(os.path.join(data_dir, formats["xml"]))
    with open(os.path.join(data_dir, formats["log"]), "w") as f:
        f.write("Log file with sample data")
    print("Sample data files generated!")

def setup_database():
    password = urllib.parse.quote_plus("Fall@2024abcd")
    engine = create_engine(f'postgresql+psycopg2://postgres:{password}@localhost:5432/intelligent_db')
    with engine.connect() as conn:
        conn.execute(text(
            "CREATE TABLE IF NOT EXISTS data_table (id SERIAL PRIMARY KEY, value FLOAT, category VARCHAR(10));"
        ))
    print("Database Schema Created!")

def etl_process():
    password = urllib.parse.quote_plus("Fall@2024abcd")
    engine = create_engine(f'postgresql+psycopg2://postgres:{password}@localhost:5432/intelligent_db')
    df = pd.read_csv(os.path.join(data_dir, formats["csv"]))
    df.columns = df.columns.str.lower()  # Convert column names to lowercase
    df = df.drop(columns=['id'])  # Exclude the 'id' column
    df.to_sql('data_table', engine, if_exists='append', index=False)
    print("ETL Process Completed")

def visualize_data():
    password = urllib.parse.quote_plus("Fall@2024abcd")
    engine = create_engine(f'postgresql+psycopg2://postgres:{password}@localhost:5432/intelligent_db')
    with engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM data_table", conn)
        plt.figure(figsize=(10, 6))
        sns.histplot(df["value"], bins=20, kde=True)
        plt.title("Distribution of Values")
        plt.show()
        print("Data Visualization Completed!")

generate_sample_data()
setup_database()
etl_process()
visualize_data()