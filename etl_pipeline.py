import pandas as pd
from datetime import datetime

# ETL Pipeline
class ETLPipeline:
    def __init__(self, input_file, output_file):
        self.input_file = input_file
        self.output_file = output_file

    def extract(self):
        """Extract data from a JSON file."""
        print(f"[{datetime.now()}] Extracting data from {self.input_file}...")
        try:
            data = pd.read_json(self.input_file)
            print(f"[{datetime.now()}] Successfully extracted data.")
            return data
        except Exception as e:
            print(f"[{datetime.now()}] Failed to extract data: {e}")
            raise

    def transform(self, data):
        """Identify and flag duplicate transactions."""
        print(f"[{datetime.now()}] Transforming data...")
        duplicate_criteria = ["Customer_ID", "Account_ID", "Branch_ID", "Transaction_DateKey", "Amount", "Transaction_Type"]
        
        # Add 'Is_Duplicate' column
        data["Is_Duplicate"] = data.duplicated(subset=duplicate_criteria, keep=False)
        
        # Log duplicate entries
        duplicates = data[data["Is_Duplicate"]]
        print(f"[{datetime.now()}] Found {len(duplicates)} duplicate transactions.")
        print(duplicates)
        
        print(f"[{datetime.now()}] Transformation complete.")
        return data

    def load(self, data):
        """Save the transformed data to an output file."""
        print(f"[{datetime.now()}] Loading data to {self.output_file}...")
        try:
            data.to_json(self.output_file, orient="records", indent=4)
            print(f"[{datetime.now()}] Successfully saved transformed data.")
        except Exception as e:
            print(f"[{datetime.now()}] Failed to save data: {e}")
            raise

    def run(self):
        """Run the ETL pipeline."""
        print(f"[{datetime.now()}] Starting ETL pipeline...")
        data = self.extract()
        transformed_data = self.transform(data)
        self.load(transformed_data)
        print(f"[{datetime.now()}] ETL pipeline complete.")

# Input and output file paths
input_file = "Fact_Transactions.json"
output_file = "Processed_Transactions.json"

# Create and run the pipeline
pipeline = ETLPipeline(input_file, output_file)
pipeline.run()
