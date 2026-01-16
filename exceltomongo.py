import pandas as pd
from pymongo import MongoClient
import numpy as np
import os

# MongoDB connection
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI)
db = client["wellbe"]

# Google Sheets CSV export links (NEW)
sheet1_url = "https://docs.google.com/spreadsheets/d/1jnxGfD_s0T2AtsQnzcPfTL-qjkMvHE3uMBEDlLdqOpA/export?format=csv&gid=0"
sheet2_url = "https://docs.google.com/spreadsheets/d/1jnxGfD_s0T2AtsQnzcPfTL-qjkMvHE3uMBEDlLdqOpA/export?format=csv&gid=1085248105"
sheet3_url = "https://docs.google.com/spreadsheets/d/1jnxGfD_s0T2AtsQnzcPfTL-qjkMvHE3uMBEDlLdqOpA/export?format=csv&gid=720888019"

def clean_dataframe_for_mongo(df):
    """Clean dataframe to handle NaN, NaT, and inf values for MongoDB insertion"""
    df = df.copy()
    
    # Replace NaN, inf, -inf with None
    df = df.replace([np.nan, np.inf, -np.inf], None)
    
    # Handle datetime columns specifically - convert NaT to None
    for col in df.columns:
        if df[col].dtype.name.startswith('datetime'):
            df[col] = df[col].where(df[col].notna(), None)
    
    return df

# Read Sheets
try:
    # Read with keep_default_na=False to preserve empty strings
    df1 = pd.read_csv(sheet1_url, keep_default_na=False, na_values=[''])
    df2 = pd.read_csv(sheet2_url, keep_default_na=False, na_values=[''])
    df3 = pd.read_csv(sheet3_url, keep_default_na=False, na_values=[''])
    
    print("Sheet 1 columns:", df1.columns.tolist())
    print("Sheet 1 shape:", df1.shape)
    print("Sheet 1 first few rows:")
    print(df1.head())
    print("\nSheet 2 columns:", df2.columns.tolist())
    print("Sheet 2 shape:", df2.shape)
    print("Sheet 2 first few rows:")
    print(df2.head())
    print("\n")
    
except Exception as e:
    print("Error fetching data:", e)
    exit()

# Clean & Format Data
## Sheet 1
# Convert Test Date to datetime
df1["Test Date"] = pd.to_datetime(df1["Test Date"], errors="coerce")

# Convert Count to numeric
df1["Count"] = pd.to_numeric(df1["Count"], errors="coerce")

# Keep W/O Division as string - replace empty strings with None
if "W/O Division" in df1.columns:
    df1["W/O Division"] = df1["W/O Division"].replace('', None)
    df1["W/O Division"] = df1["W/O Division"].replace(np.nan, None)

# Filter out rows where Test Date is NaT (None/NaN)
df1 = df1[df1["Test Date"].notna()]

df1 = clean_dataframe_for_mongo(df1)

## Sheet 2
# Convert numeric columns
df2["Tests Count"] = pd.to_numeric(df2["Tests Count"], errors="coerce")
df2["Sample Processed"] = pd.to_numeric(df2["Sample Processed"], errors="coerce")
df2["Report Printed"] = pd.to_numeric(df2["Report Printed"], errors="coerce")
df2["Report Distributed"] = pd.to_numeric(df2["Report Distributed"], errors="coerce")

# Keep the first column as string (whatever it is named)
first_col_name = df2.columns[0]
df2[first_col_name] = df2[first_col_name].replace('', None)
df2[first_col_name] = df2[first_col_name].replace(np.nan, None)

df2 = clean_dataframe_for_mongo(df2)

## Sheet 3
df3.columns = [
    "District", "% Risk of Hypertension", "Hypertension Count", "% Risk of Diabetes", "Diabetes Count",
    "Prevalence of Anemia", "Anemia Count", "Thyroid Status", "Thyroid Count",
    "Chronic Kidney Disease", "CKD Count", "Liver Disease", "Liver Disease Count",
    "Need Spectacles", "Spectacles Count", "Need Aid", "Aid Count"
]

numeric_cols = [
    "Hypertension Count", "Diabetes Count", "Anemia Count", "Thyroid Count",
    "CKD Count", "Liver Disease Count", "Spectacles Count", "Aid Count"
]
for col in numeric_cols:
    df3[col] = pd.to_numeric(df3[col], errors="coerce")

df3 = clean_dataframe_for_mongo(df3)
df3 = df3.dropna(subset=["District"])

# Convert DataFrames to records and ensure all values are MongoDB-compatible
def prepare_records_for_mongo(df):
    """Convert DataFrame to records with MongoDB-compatible values"""
    records = df.to_dict("records")
    
    # Additional cleaning for each record
    cleaned_records = []
    for record in records:
        cleaned_record = {}
        for key, value in record.items():
            # Handle pandas._libs.tslibs.nattype.NaTType specifically
            if pd.isna(value):
                cleaned_record[key] = None
            elif isinstance(value, pd.Timestamp):
                # Convert pandas Timestamp to datetime, handle NaT
                if pd.isna(value):
                    cleaned_record[key] = None
                else:
                    cleaned_record[key] = value.to_pydatetime()
            elif isinstance(value, (np.int64, np.int32, np.int16, np.int8)):
                # Convert numpy int to Python int
                cleaned_record[key] = int(value)
            elif isinstance(value, (np.float64, np.float32, np.float16)):
                # Convert numpy float to Python float
                cleaned_record[key] = float(value)
            elif value == '':
                # Convert empty strings to None
                cleaned_record[key] = None
            else:
                cleaned_record[key] = value
        cleaned_records.append(cleaned_record)
    
    return cleaned_records

# Insert into MongoDB
try:
    # Sheet 1
    db["command_center_ds"].delete_many({})
    records1 = prepare_records_for_mongo(df1)
    if records1:
        db["command_center_ds"].insert_many(records1)
        print(f"✓ Inserted {len(records1)} records to command_center_ds")
    else:
        print("⚠ No valid records to insert for command_center_ds (all Test Dates were missing)")
    
    # Sheet 2
    db["command_center_report"].delete_many({})
    records2 = prepare_records_for_mongo(df2)
    if records2:
        db["command_center_report"].insert_many(records2)
        print(f"✓ Inserted {len(records2)} records to command_center_report")
    
    # Sheet 3
    db["command_center_care"].delete_many({})
    records3 = prepare_records_for_mongo(df3)
    if records3:
        db["command_center_care"].insert_many(records3)
        print(f"✓ Inserted {len(records3)} records to command_center_care")
    
    print("\n✓ Data updated successfully in MongoDB!")

except Exception as e:
    print(f"✗ Error inserting data to MongoDB: {e}")
    import traceback
    traceback.print_exc()

finally:
    client.close()