import os
import pandas as pd
from datetime import datetime
import time
import psutil
from multiprocessing import Pool
import yagmail


def load_active_records(config_file):
    """
    Load and filter only active records based on the configuration table.
    """
    config_df = pd.read_csv(config_file)
    active_config = config_df[config_df['Enable'] == 'Y']

    if active_config.empty:
        raise ValueError("No active operations found in the configuration table.")

    return active_config

def load_column_mapping(mapping_file, table_name):
    """
    Load column mapping for a specific table from a CSV file.
    :param mapping_file: Path to the column mapping file.
    :param table_name: Name of the table to filter the mapping for.
    :return: Dictionary mapping source column names to target column names.
    """
    mapping_df = pd.read_csv(mapping_file)
    table_mapping = mapping_df[mapping_df['Table_Name'] == table_name]
    if table_mapping.empty:
        raise ValueError(f"No column mapping found for table '{table_name}' in the mapping file.")
    
    # Extract source and target columns
    source_columns = table_mapping['Source_Columns'].iloc[0].split(',')
    target_columns = table_mapping['Target_Columns'].iloc[0].split(',')
    
    if len(source_columns) != len(target_columns):
        raise ValueError(f"Mismatch in the number of source and target columns for table '{table_name}'.")
    
    return dict(zip(target_columns, source_columns))

# def append_test_execution_result(test_execution_file, test_case_id, test_case_description,table_name, execution_status, execution_details):
#     """
#     Append test execution results to the TEST_EXECUTION_RESULT CSV file.
#     """
#     current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#     test_execution_id = pd.read_csv(test_execution_file).shape[0] + 1  # Auto-increment ID
#     if execution_status=="Passed":
#         ACTIVE_FLAG="N"
#     else:
#         ACTIVE_FLAG="Y"
#     new_result = {
#         'TEST_EXECUTION_ID': test_execution_id,
#         'TEST_CASE_ID': test_case_id,
#         'TEST_CASE_DESCRIPTION': test_case_description,
#         'TABLE_NAME':table_name,
#         'EXECUTION_STATUS': execution_status,
#         'EXECUTION_DETAILS': execution_details,
#         'ACTIVE_FLAG': ACTIVE_FLAG,
#         'CREATED_DATE': current_time
#     }   
#     test_execution_df = pd.read_csv(test_execution_file)
#     new_result_df = pd.DataFrame([new_result])
#     test_execution_df = pd.concat([test_execution_df, new_result_df], ignore_index=True)
#     test_execution_df.to_csv(test_execution_file, index=False)

def prepare_test_execution_result(test_execution_id,test_case_id, test_case_description, table_name, execution_status, execution_details):
    """
    Prepare test execution results as a dictionary.
    """
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    ACTIVE_FLAG = "N" if execution_status == "Passed" else "Y"
    return {
        'TEST_EXECUTION_ID': test_execution_id,
        'TEST_CASE_ID': test_case_id,
        'TEST_CASE_DESCRIPTION': test_case_description,
        'TABLE_NAME': table_name,
        'EXECUTION_STATUS': execution_status,
        'EXECUTION_DETAILS': execution_details,
        'ACTIVE_FLAG': ACTIVE_FLAG,
        'CREATED_DATE': current_time
    }

def compare_row_counts(results,test_execution_id, source_df, target_df, source_table, target_table):
    source_count = len(source_df)
    target_count = len(target_df)
    execution_details = f"Source ({source_table}) Row Count: {source_count}, Target ({target_table}) Row Count: {target_count}"
    execution_status = "Passed" if source_count == target_count else "Failed"

    # append_test_execution_result(test_execution_file, 1, "Row Count Comparison",target_table, execution_status, execution_details)
    results.append(prepare_test_execution_result(test_execution_id,1, "Row Count Comparison", target_table, execution_status, execution_details))

def compare_row_by_row_sorted_chunks_nested_for(results,test_execution_id, source_df, target_df, source_table, target_table, output_folder, chunksize=50000, id_columns=None):
    """
    Perform row-by-row comparison in chunks without nested loops by sorting and aligning chunks.
    Compare only the common rows between source and target DataFrames.
    """
    try:
        mismatch_records = []
        now = datetime.now()
        # Get the next TEST_EXECUTION_ID from the TEST_EXECUTION_RESULT file
        # if os.path.exists(test_execution_file):
        #     log_df = pd.read_csv(test_execution_file)
        #     run_id = log_df['TEST_EXECUTION_ID'].max() + 1  # Increment the last RunID
        # else:
        #     run_id = 1 
         

        # Remove duplicates from the target DataFrame
        if id_columns and len(id_columns) > 1:
            # Create a composite key in both DataFrames
            source_df['Composite_Key'] = source_df[id_columns].astype(str).agg('_'.join, axis=1)
            target_df['Composite_Key'] = target_df[id_columns].astype(str).agg('_'.join, axis=1)
            key_column = 'Composite_Key'
            
        elif id_columns and len(id_columns) == 1:
            # Use the single column as the key
            key_column = id_columns[0]
        # Remove duplicates from the target DataFrame based on the composite key
        if key_column in target_df.columns:
            duplicate_rows = target_df[target_df.duplicated(subset=key_column, keep=False)]
            if not duplicate_rows.empty:
                duplicates_folder = os.path.join(output_folder, "Duplicates")
                os.makedirs(duplicates_folder, exist_ok=True)
                duplicate_file = os.path.join(duplicates_folder, f"{target_table}_duplicates.csv")
                duplicate_rows[key_column].to_csv(duplicate_file, index=False,header=["Duplicate_Row_IDs"])
                # append_test_execution_result(test_execution_file, 3, "Duplicate Data",target_table, "Found Duplicates", f"Duplicates found. Details saved in {duplicate_file}.")
                results.append(prepare_test_execution_result(test_execution_id,3, "Duplicate Data", target_table, "Failed", f"{len(duplicate_rows)}_Duplicates found. Details saved in {duplicate_file}."))

            # Drop duplicates, keeping the first occurrence
                target_df = target_df.drop_duplicates(subset=key_column, keep='first')
                source_df = source_df.drop_duplicates(subset=key_column, keep='first')

         # Identify missing rows in the target
        if key_column in source_df.columns and key_column in target_df.columns:
            missing_keys = set(source_df[key_column]) - set(target_df[key_column])
            missing_rows = source_df[source_df[key_column].isin(missing_keys)]
            
        else:
            raise ValueError(f"Key column '{key_column}' not found in both DataFrames.")

        # Log missing rows
        if not missing_rows.empty:
            missing_folder = os.path.join(output_folder, "Missing_Rows")
            os.makedirs(missing_folder, exist_ok=True)
            missing_file = os.path.join(missing_folder, f"{source_table}_missing_in_target.csv")
            missing_rows.to_csv(missing_file, index=False)
            results.append(prepare_test_execution_result(
                test_execution_id, 4, "Missing Rows in Target", target_table, "Failed",
                f"{len(missing_rows)} rows missing in target. Details saved in {missing_file}."
            ))

        # Identify common rows based on the composite key
        if key_column in source_df.columns and key_column in target_df.columns:
            common_keys = set(source_df[key_column]).intersection(set(target_df[key_column]))
            source_df = source_df[source_df[key_column].isin(common_keys)]
            target_df = target_df[target_df[key_column].isin(common_keys)]
        else:
            raise ValueError(f"Key column '{key_column}' not found in both DataFrames.")
        # Sort the filtered DataFrames by the specified columns
        source_df = source_df.sort_values(by=id_columns).reset_index(drop=True)
        target_df = target_df.sort_values(by=id_columns).reset_index(drop=True)

        # Split the sorted DataFrames into chunks
        source_chunks = [source_df.iloc[i:i + chunksize] for i in range(0, len(source_df), chunksize)]
        target_chunks = [target_df.iloc[i:i + chunksize] for i in range(0, len(target_df), chunksize)]

        # Compare chunks
        for source_chunk, target_chunk in zip(source_chunks, target_chunks):
            # Ensure the chunks have the same shape
            if source_chunk.shape != target_chunk.shape:
                print("Source and target chunks have different shapes. Skipping this chunk.")
                continue

            # Perform element-wise comparison
            mismatched_rows = source_chunk != target_chunk
            source_columns = source_chunk.columns
            # Iterate over mismatched columns
            for col in source_columns:
                
                mismatched_col = mismatched_rows[mismatched_rows[col]]
                if not mismatched_col.empty:
                    for row_id, src, tgt in zip(
                        source_chunk.loc[mismatched_col.index, key_column],  # Unique ID column
                        source_chunk.loc[mismatched_col.index, col],  # Source value
                        target_chunk.loc[mismatched_col.index, col]   # Target value
                    ):
                        mismatch_records.append((
                            f"{source_table}",
                            f"Data Mismatch -  {col}",
                            f"RowID: {row_id}, Source: {src}, Target: {tgt}",
                        ))
            
        
        # Save mismatches to a file if any are found
        if mismatch_records:
            os.makedirs(output_folder, exist_ok=True)
            mismatch_file = os.path.join(output_folder, f"{source_table}_runid_{test_execution_id}.csv")
            mismatch_df = pd.DataFrame(mismatch_records, columns=['Table', 'Mimatch', 'Details'])
            mismatch_df.to_csv(mismatch_file, index=False)
            execution_status = "Failed"
            execution_details = f"Mismatches found. Details saved in {mismatch_file}. and {len(mismatch_records)} mismatches found."
        else:
            execution_status = "Passed"
            execution_details = "No mismatches found."

        # Append the test execution result
        # append_test_execution_result(test_execution_file, 2, "Data Comparison (Sorted Chunks)",target_table, execution_status, execution_details)
        results.append(prepare_test_execution_result(test_execution_id,2, "Data Comparison (Sorted Chunks)", target_table, execution_status, execution_details))
    except Exception as e:
        print(f"An error occurred during sorted chunk-based row-by-row comparison: {e}")

def send_email_with_yagmail(sender_email,sender_password, recipient_email, subject, body):
    """
    Send an email with the reports attached using yagmail.
    :param sender_email: Sender's email address.
    :param recipient_email: Recipient's email address.
    :param subject: Subject of the email.
    :param report_folder: Path to the folder containing the reports.
    """
    try:
        # Initialize yagmail
        yag = yagmail.SMTP(sender_email,sender_password)

        yag.send(
            to=recipient_email,
            subject=subject,
            contents=body
        )

        print("Email sent successfully.")
    except Exception as e:
        print(f"An error occurred while sending the email: {e}")


def process_single_table_pair(args):
    """
    Process a single source-target table pair.
    """
    try:
        (test_execution_id,source_table, target_table, sort_columns, source_file_path, target_file_path, 
          output_folder, chunksize,mapping_file) = args
        results = []  # Local results list for this process
        start_time = time.time()
        if not os.path.exists(source_file_path):
            print(f"Source file {source_file_path} does not exist. Skipping.")
            return
        if not os.path.exists(target_file_path):
            print(f"Target file {target_file_path} does not exist. Skipping.")
            return

        source_df = pd.read_csv(source_file_path)
        target_df = pd.read_csv(target_file_path)
        

        # Load column mapping for the current table
        column_mapping = load_column_mapping(mapping_file, source_table)

        # Filter source and target DataFrames to include only the mapped columns
        source_df = source_df[list(column_mapping.values())]
        target_df = target_df[list(column_mapping.keys())]


        # Align target columns using the mapping
        target_df.rename(columns=column_mapping, inplace=True)
        
        # Compare row counts
        compare_row_counts(results,test_execution_id, source_df, target_df, source_table, target_table)

        # Perform row-by-row comparison
        compare_row_by_row_sorted_chunks_nested_for(
            results=results,
            test_execution_id=test_execution_id,
            source_df=source_df,
            target_df=target_df,
            source_table=source_table,
            target_table=target_table,
            output_folder=output_folder,
            chunksize=chunksize,
            id_columns=sort_columns
        )
        end_time = time.time()

        print(f"Time taken for {source_table} : {end_time - start_time:.2f} seconds")
        return results  # Return the results for this process   

    except Exception as e:
        print(f"An error occurred while processing {source_table} and {target_table}: {e}")


def process_operations(config_file, source_folder, target_folder, output_folder, test_execution_file,mapping_file):
    try:
        print("Starting data comparison operations...")
        active_config = load_active_records(config_file)
        # Determine the starting TEST_EXECUTION_ID
        if os.path.exists(test_execution_file):
            test_execution_df = pd.read_csv(test_execution_file)
            if not test_execution_df.empty:
                start_test_execution_id = test_execution_df['TEST_EXECUTION_ID'].max() + 1
            else:
                start_test_execution_id = 1
        else:
            start_test_execution_id = 1
        test_execution_id = start_test_execution_id  # Start from the last ID in the file
        
        tasks = []
        for _, row in active_config.iterrows():
            source_table = row['Source Tablename']
            target_table = row['Target Tablename']
            sort_columns = row['Sort_Columns'].split(',')

            source_file_path = os.path.join(source_folder, f"{source_table}.csv")
            target_file_path = os.path.join(target_folder, f"{target_table}.csv")

            tasks.append((test_execution_id,source_table, target_table, sort_columns, source_file_path, target_file_path, output_folder, 500000,mapping_file))
            test_execution_id += 1  # Increment the ID for the next task

        # Use multiprocessing to process table pairs
        with Pool(processes=os.cpu_count()) as pool:  # Use all available CPU cores
            results_from_processes=pool.map(process_single_table_pair, tasks,chunksize=1)
        # Close the pool to free up resources
        pool.close()
        pool.join()
        # Aggregate results from all processes
        aggregated_results = [result for process_results in results_from_processes for result in process_results]

        # Write all results to the test execution file
        if aggregated_results:
            test_execution_df = pd.read_csv(test_execution_file) if os.path.exists(test_execution_file) else pd.DataFrame()
            new_results_df = pd.DataFrame(aggregated_results)
            test_execution_df = pd.concat([test_execution_df, new_results_df], ignore_index=True)
            test_execution_df.to_csv(test_execution_file, index=False)


        print("Completed all operations.")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    # File paths
    config_file = 'config.csv'
    source_folder = 'source_files'
    target_folder = 'target_files'
    output_folder = 'mismatch'
    test_execution_file = 'test_execution_result.csv'
    mapping_file = 'column_mapping.csv'

    start_time = time.time()
    process = psutil.Process(os.getpid())
    start_memory = process.memory_info().rss / (1024 * 1024)  # Memory in MB

    # Perform the operations
    process_operations(config_file, source_folder, target_folder, output_folder, test_execution_file,mapping_file)

    end_time = time.time()
    end_memory = process.memory_info().rss / (1024 * 1024)  # Memory in MB
    print(f"Time taken: {end_time - start_time:.2f} seconds")
    print(f"Memory used: {end_memory - start_memory:.2f} MB")
    
    # Email details
    sender_email = "mouryaworkspace@gmail.com"
    sender_password = "ctde yiuh xcde fuwr"  
    recipient_email = "mouryaworkspace@gmail.com"
    subject = "Data Comparison Reports"
    body = f"""
    Hi,

    The data comparison process has been completed. Please find the reports attached.

    Report Folder: {os.path.abspath(output_folder)}

    Time Taken: {end_time - start_time:.2f} seconds
    Memory Used: {end_memory - start_memory:.2f} MB

    Regards,
    Data Comparison Tool
    """

    # Send the email with the reports
    send_email_with_yagmail(sender_email,sender_password, recipient_email, subject, body)