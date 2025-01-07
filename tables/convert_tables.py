import pandas as pd

# List of input JSON files and corresponding output CSV files
files = [
    {"input": "Fact_Transactions.json", "output": "Fact_Transactions.csv"},
    {"input": "Dim_Customers.json", "output": "Dim_Customers.csv"},
    {"input": "Dim_Accounts.json", "output": "Dim_Accounts.csv"},
    {"input": "Dim_Branches.json", "output": "Dim_Branches.csv"},
    {"input": "Dim_Date.json", "output": "Dim_Date.csv"},
]

# Process each file
for file in files:
    try:
        # Read JSON into a DataFrame
        df = pd.read_json(file["input"])
        # Save the DataFrame to a CSV file
        df.to_csv(file["output"], index=False)
        print(f"Successfully transformed {file['input']} to {file['output']}")
    except Exception as e:
        print(f"Error processing {file['input']}: {e}")
