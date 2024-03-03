import os
import logging
import structlog
from structlog.processors import JSONRenderer, TimeStamper
from datetime import datetime
import pandas as pd
import re
from tqdm import tqdm

def configure_logging():
    # Define the filename with a timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"{timestamp}_logfile.log"
    
    # Define the directory and ensure it exists
    log_directory = "logs"
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)
    
    # Full path for the log file
    run_log_full_filepath = os.path.join(log_directory, filename)
    
    # Clear existing handlers to avoid duplication
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    # Configure basic logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        handlers=[logging.FileHandler(run_log_full_filepath), logging.StreamHandler()],
    )
    
    # Configure structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            # Ensure the final processor is JSONRenderer for structured logging
            JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    # Return the configured logger
    return structlog.get_logger()

# Configure the logger
logger = configure_logging()

# Corrected logging call with keyword arguments
logger.info(event="Starting process", detail="Log setup successful")

# List of Excel file paths
file_paths = [
    'data/Export 01_03_2024 13_35.xlsx',
    'data/Export 01_03_2024 13_38.xlsx',
    'data/Export 01_03_2024 13_41.xlsx',
    'data/Export 01_03_2024 13_45.xlsx',
    'data/Export 01_03_2024 13_48.xlsx',
    'data/Export 01_03_2024 13_52.xlsx',
    'data/Export 01_03_2024 13_55.xlsx',
    # Assuming the contents of 'drive-download-20240303T102932Z-001.zip' are extracted and paths added here
]

logger.info("Reading Excel files", file_count=len(file_paths))

# Read and union all Excel files into a single DataFrame
dfs = []

for file_path in file_paths:
    logger.info("Reading file", file_path=file_path)
    df = pd.read_excel(file_path, sheet_name='Results')
    # Add an 'origin' column to each DataFrame
    df['origin'] = os.path.basename(file_path)
    dfs.append(df)

# Concatenate all DataFrames, keeping the 'origin' column
union_df = pd.concat(dfs, ignore_index=True)

logger.info("Union completed", total_rows=union_df.shape[0])

xlsx_filename = f"UnionedDataFrame.xlsx"
xlsx_full_path = os.path.join("data", xlsx_filename)  # Ensure the 'data' directory exists or adjust the path as needed
union_df.to_excel(xlsx_full_path, index=False, engine='openpyxl')
logger.info("Saved unioned DataFrame with origin as Excel", file_name=xlsx_filename)

def extract_year(column_name):
    """Extracts the year from column names, considering various formats."""
    match = re.search(r'(\d{4})\/\d{4}$', column_name)
    if match:
        return match.group(1), re.sub(r'\s*\d{4}\/\d{4}$', '', column_name).strip()
    else:
        match = re.search(r'(\d{4})\/?$', column_name)
        if match:
            return match.group(1), re.sub(r'\s*\d{4}\/?$', '', column_name).strip()
    return None, column_name.strip()

new_df_data = []

logger.info("Processing unified DataFrame")

# Process the unified DataFrame
for _, row in tqdm(union_df.iterrows(), total=union_df.shape[0], desc="Processing Rows"):
    year_data = {}
    for col in union_df.columns:
        year, new_col_name = extract_year(col)
        if year:
            key = (row['Company name Latin alphabet'], year)
            if key not in year_data:
                year_data[key] = {'Company name Latin alphabet': row['Company name Latin alphabet'], 'Year': year}
            year_data[key][new_col_name] = row[col]
        else:
            if 'Company name Latin alphabet' not in year_data:
                year_data['base'] = {col: row[col] for col in union_df.columns if not re.search(r'\d{4}\/?\d{4}$', col)}
    for record in year_data.values():
        new_df_data.append(record)

# Remove 'base' records and convert to DataFrame
new_df_data = [record for record in new_df_data if 'Year' in record]
new_df = pd.DataFrame(new_df_data)

# Group by 'Company name Latin alphabet' and 'Year' if needed
new_df = new_df.groupby(['Company name Latin alphabet', 'Year']).first().reset_index()

# Identifying fields not present in all files
columns_with_missing_data = new_df.columns[new_df.isnull().any()].tolist()

# Print out the columns that were not present in all the files
logger.info("Columns not present in all files", missing_columns=columns_with_missing_data)

# Saving the processed DataFrame
new_df.to_excel('Unified_and_Processed_Records.xlsx', index=False, sheet_name='Results')
logger.info("Saving processed DataFrame", file_name='Unified_and_Processed_Records.xlsx')

# Make sure to close the log file at the end of the script
logger.info("Process completed")
