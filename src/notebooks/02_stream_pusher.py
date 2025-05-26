import pandas as pd
import time
import os

CSV_PATH = "FileStore/tables/sales_data.csv"
STREAM_DIR = "/dbfs/tmp/j4_databrick_streaming/"
DELAY_SECONDS = 2  

def ensure_directory_exists(directory_path):
    """Create directory if it doesn't exist"""
    dbutils.fs.mkdirs(directory_path.replace("/dbfs", ""))

def push_data_as_stream(df, output_dir, delay_seconds):
    """Push data row by row with delay to simulate streaming"""
    for i, row in df.iterrows():
        temp = row.to_frame().transpose()
        output_file = os.path.join(output_dir, f"part_{i}.csv")
        
        # Write with header only for first file
        temp.to_csv(output_file, header=(i==0), index=False, mode='w')
        
        # Log progress
        if i % 10 == 0:
            print(f"Pushed {i} records to streaming directory")
            
        # Delay to simulate streaming
        time.sleep(delay_seconds)

def main():
    # Read source data
    df = pd.read_csv(CSV_PATH)
    
    # Ensure output directory exists
    ensure_directory_exists(STREAM_DIR)
    
    # Push data as stream
    print(f"Starting to push {len(df)} records to {STREAM_DIR}")
    push_data_as_stream(df, STREAM_DIR, DELAY_SECONDS)
    print("Streaming simulation complete")

# Execute main function
main()
