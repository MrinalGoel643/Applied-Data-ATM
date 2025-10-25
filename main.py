import pandas as pd 

import os

# Define file paths (adjust as needed)
base_path = r"Type the Path"

# Read parquet files
df_a = pd.read_parquet(os.path.join(base_path, "advances.parquet"))
df_b = pd.read_parquet(os.path.join(base_path, "balances.parquet"))
df_l = pd.read_parquet(os.path.join(base_path, "labels.parquet"))
df_a = pd.read_parquet(os.path.join(base_path, "applications.parquet"))
df_t = pd.read_parquet(os.path.join(base_path, "transactions_sample.parquet"))

# Function to ensure datetime format retention
def format_datetime_columns(df):
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            # Convert to string with full precision
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S.%f')
    return df

# Apply formatting
df_a = format_datetime_columns(df_a)
df_b = format_datetime_columns(df_b)
df_l = format_datetime_columns(df_l)

# Save to CSV (exact datetime strings preserved)
#df_a.to_csv(os.path.join(base_path, "Advances.csv"), index=False)
#df_b.to_csv(os.path.join(base_path, "Balances.csv"), index=False)
#df_l.to_csv(os.path.join(base_path, "Labels.csv"), index=False)
df_b.to_csv(os.path.join(base_path, "Applications.csv"), index=False)
df_l.to_csv(os.path.join(base_path, "Transactions.csv"), index=False)


