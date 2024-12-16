import pandas as pd

file_path = "/home/developer/NAYA_Final_Project/testing_parquet_files/part-00000-dc674e93-f4ca-43dc-b15b-6af5589d0e7c.c000.snappy.parquet"

try:
    data = pd.read_parquet(file_path)
    print("File loaded successfully!")

    print("\nBasic Information:")
    print("Columns:", data.columns.tolist())
    print("Row Count:", len(data))

    print("\nPreview:")
    print(data.head())
except Exception as e:
    print(f"Error: {e}")

print(data)
