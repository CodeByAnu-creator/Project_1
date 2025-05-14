# data_extraction.py

# Import Libraries
import json
import os
import pandas as pd
from sqlalchemy import create_engine, types
# Import necessary credentials including the encoded password
from credentials import DB_USER, ENCODED_PASSWORD, DB_HOST, DB_PORT, DB_DATABASE


# --- Data Extraction Functions ---

def process_agg_transaction_data(path):
    extracted_data = []
    for state in os.listdir(path):
        state_path = os.path.join(path, state)
        if os.path.isdir(state_path):
            for year in os.listdir(state_path):
                year_path = os.path.join(state_path, year)
                if os.path.isdir(year_path):
                    for quarter_file in os.listdir(year_path):
                        quarter_path = os.path.join(year_path, quarter_file)
                        if quarter_file.endswith('.json'):
                            try:
                                quarter = int(quarter_file.split('.')[0])
                                with open(quarter_path, 'r') as json_file:
                                    data = json.load(json_file)
                                    if data and 'data' in data and 'transactionData' in data['data']:
                                        for transaction in data['data']['transactionData']:
                                            transaction_type = transaction['name']
                                            for instrument in transaction['paymentInstruments']:
                                                if instrument['type'] == 'TOTAL':
                                                    count = instrument['count']
                                                    amount = instrument['amount']
                                                    extracted_data.append({
                                                        'State': state,
                                                        'Year': int(year),
                                                        'Quarter': quarter,
                                                        'TransactionType': transaction_type,
                                                        'TransactionCount': count,
                                                        'TransactionAmount': amount
                                                    })
                                    else:
                                         print(f"Warning: Unexpected data structure in {quarter_path}")
                            except Exception as e:
                                print(f"Error processing file {quarter_path}: {e}")
    return extracted_data
'''
def process_agg_user_data(path):
    extracted_data = []
    for state in os.listdir(path):
        state_path = os.path.join(path, state)
        if os.path.isdir(state_path):
            for year in os.listdir(state_path):
                year_path = os.path.join(state_path, year)
                if os.path.isdir(year_path):
                    for quarter_file in os.listdir(year_path):
                        quarter_path = os.path.join(year_path, quarter_file)
                        if quarter_file.endswith('.json'):
                            try:
                                with open(quarter_path, 'r') as json_file:
                                    data = json.load(json_file)
                                    quarter = int(quarter_file.split('.')[0])

                                    # Extract registeredUsers directly
                                    registered_users_data = data.get('data', {}).get('aggregated', {})

                                    # Append data for registered users
                                    if registered_users_data and 'registeredUsers' in registered_users_data:
                                         registered_users = registered_users_data['registeredUsers']
                                         extracted_data.append({
                                             'State': state,
                                             'Year': int(year),
                                             'Quarter': quarter,
                                             'RegisteredUsers': registered_users
                                         })
                                    else:
                                        # Handle cases where registeredUsers might be missing in some files
                                        # print(f"Warning: 'registeredUsers' not found in {quarter_path}")
                                        pass # Or handle as needed

                            except Exception as e:
                                print(f"Error processing file {quarter_path}: {e}")
    return extracted_data
'''
def process_agg_user_data(path):
    extracted_data = []

    # Check if the base path exists
    if not os.path.exists(path):
        print(f"Error: Base path not found: {path}")
        return extracted_data
    if not os.path.isdir(path):
         print(f"Error: Base path is not a directory: {path}")
         return extracted_data


    for state_name in os.listdir(path):
        state_path = os.path.join(path, state_name)

        # Ensure it's a directory
        if os.path.isdir(state_path):
            for year_str in os.listdir(state_path):
                year_path = os.path.join(state_path, year_str)

                # Ensure it's a directory and year is convertible to int
                try:
                    year = int(year_str)
                except ValueError:
                    # print(f"Skipping non-integer directory name: {year_str} in {state_path}") # Uncomment for debugging
                    continue

                if os.path.isdir(year_path):
                    for quarter_file in os.listdir(year_path):
                        quarter_path = os.path.join(year_path, quarter_file)

                        # Process only JSON files with names like '1.json', '2.json', etc.
                        if quarter_file.endswith('.json'):
                            try:
                                # Extract quarter number from filename (e.g., '1.json' -> 1)
                                quarter_str = quarter_file.split('.')[0]
                                quarter = int(quarter_str)

                                # Validate quarter number
                                if not 1 <= quarter <= 4:
                                     # print(f"Skipping file with invalid quarter number: {quarter_file}") # Uncomment for debugging
                                     continue

                                with open(quarter_path, 'r', encoding='utf-8') as json_file:
                                     data = json.load(json_file)

                                # --- Extraction Logic for Brand Data and Total Registered Users ---

                                # Extract total registered users safely
                                registered_users_total = data.get('data', {}).get('aggregated', {}).get('registeredUsers')

                                # Extract brand data list safely
                                brand_data_list = data.get('data', {}).get('usersByDevice', [])

                                # Ensure the extracted data is a list
                                if not isinstance(brand_data_list, list):
                                    print(f"Warning: Expected 'usersByDevice' to be a list, found {type(brand_data_list)} in {quarter_path}. Skipping brand data.")
                                    brand_data_list = [] # Treat as empty list if not a list

                                # Iterate through each brand entry in the list
                                for brand_entry in brand_data_list:
                                    # Ensure the entry is a dictionary
                                    if not isinstance(brand_entry, dict):
                                        print(f"Warning: Expected brand entry to be a dictionary, found {type(brand_entry)} in {quarter_path}. Skipping entry.")
                                        continue

                                    # Extract brand details safely
                                    brand = brand_entry.get('brand')
                                    count = brand_entry.get('count')
                                    percentage = brand_entry.get('percentage')

                                    # Validate extracted brand data before appending
                                    if brand is not None and count is not None and percentage is not None:
                                         try:
                                              # Ensure count and percentage are numeric types
                                              count = int(count)
                                              percentage = float(percentage)

                                              # Append dictionary including total registered users
                                              extracted_data.append({
                                                  'state': state_name,
                                                  'year': year,
                                                  'quarter': quarter,
                                                  'brand': str(brand),
                                                  'count': count,
                                                  'percentage': percentage,
                                                  'registeredUsers': int(registered_users_total) if registered_users_total is not None else None # Include total, convert to int if found
                                              })
                                         except (ValueError, TypeError) as e:
                                              print(f"Error converting data types in {quarter_path} for brand {brand}: {e}. Skipping entry.")
                                    # else:
                                        # print(f"Warning: Missing 'brand', 'count', or 'percentage' in an entry in {quarter_path}. Skipping entry: {brand_entry}") # Uncomment for debugging missing keys

                            except json.JSONDecodeError:
                                print(f"Error decoding JSON file: {quarter_path}")
                            except FileNotFoundError:
                                # Should not happen if os.listdir is used correctly, but good practice
                                print(f"Error: File not found: {quarter_path}")
                            except Exception as e:
                                # Catch any other unexpected errors during file processing
                                print(f"An unexpected error occurred processing file {quarter_path}: {e}")

    print(f"Finished processing. Extracted {len(extracted_data)} brand-level entries (including total registered users).")
    return extracted_data



'''
# --- New Function for Aggregated Insurance Data ---
def process_agg_insurance_data(path):
    extracted_data = []
    for state in os.listdir(path):
        state_path = os.path.join(path, state)
        if os.path.isdir(state_path):
            for year in os.listdir(state_path):
                year_path = os.path.join(state_path, year)
                if os.path.isdir(year_path):
                    for quarter_file in os.listdir(year_path):
                        quarter_path = os.path.join(year_path, quarter_file)
                        if quarter_file.endswith('.json'):
                            try:
                                quarter = int(quarter_file.split('.')[0])
                                with open(quarter_path, 'r') as json_file:
                                    data = json.load(json_file)
                                    # Assuming insurance data follows a similar structure to transactions
                                    # Path: data -> insuranceData -> list of insurance types/metrics
                                    if data and 'data' in data and 'insuranceData' in data['data'] and isinstance(data['data']['insuranceData'], list):
                                        for insurance_metric in data['data']['insuranceData']:
                                            # Assuming each item in insuranceData has 'name', 'count', 'amount'
                                            insurance_type = insurance_metric.get('name') # Use .get for safety
                                            count = insurance_metric.get('count')
                                            amount = insurance_metric.get('amount')

                                            if insurance_type and count is not None and amount is not None:
                                                extracted_data.append({
                                                    'State': state,
                                                    'Year': int(year),
                                                    'Quarter': quarter,
                                                    'InsuranceType': insurance_type,
                                                    'InsuranceCount': count,
                                                    'InsuranceAmount': amount
                                                })
                                            else:
                                                # print(f"Warning: Missing 'name', 'count', or 'amount' in insurance metric in {quarter_path}: {insurance_metric}")
                                                pass # Suppress frequent warnings

                                    else:
                                        # print(f"Warning: Unexpected data structure or missing 'insuranceData' list in {quarter_path}")
                                        pass # Suppress frequent warnings
                            except Exception as e:
                                print(f"Error processing file {quarter_path}: {e}")
    return extracted_data
'''
def process_agg_insurance_data(path):
    extracted_data = []
    # print(f"Starting processing for path: {path}") # Optional debug print

    # Loop through each state directory
    for state in os.listdir(path):
        state_path = os.path.join(path, state)

        # Ensure it's a directory before proceeding
        if os.path.isdir(state_path):
            # Loop through each year directory within the state
            for year in os.listdir(state_path):
                year_path = os.path.join(state_path, year)

                # Ensure it's a directory before proceeding
                if os.path.isdir(year_path):
                    # Loop through each file within the year directory (quarter files)
                    for quarter_file in os.listdir(year_path):
                        quarter_path = os.path.join(year_path, quarter_file)

                        # Process only JSON files
                        if quarter_file.endswith('.json'):
                            try:
                                # Extract the quarter number from the filename (e.g., '1.json' -> 1)
                                quarter = int(quarter_file.split('.')[0])

                                # Open and load the JSON file
                                with open(quarter_path, 'r') as json_file:
                                    data = json.load(json_file)

                                # --- Data Extraction Logic based on sample JSON ---
                                # The sample shows insurance is under 'transactionData', NOT 'insuranceData'
                                # Path: data -> data -> transactionData -> list of items (one of which is "Insurance")
                                if data and 'data' in data and isinstance(data['data'], dict) and \
                                   'transactionData' in data['data'] and isinstance(data['data']['transactionData'], list):

                                    # Iterate through the items found under 'transactionData'
                                    for item_data in data['data']['transactionData']:
                                        # Check if this specific item is the 'Insurance' entry
                                        if item_data.get('name') == 'Insurance':
                                            # If it's the Insurance entry, look for its 'paymentInstruments' list
                                            payment_instruments = item_data.get('paymentInstruments')

                                            if isinstance(payment_instruments, list):
                                                 # Iterate through the instruments to find the 'TOTAL' type
                                                 for instrument in payment_instruments:
                                                      if instrument.get('type') == 'TOTAL':
                                                         # Extract count and amount from the 'TOTAL' instrument
                                                         count = instrument.get('count')
                                                         amount = instrument.get('amount')

                                                         # Append the extracted data if count and amount are present
                                                         if count is not None and amount is not None:
                                                             extracted_data.append({
                                                                 'State': state,
                                                                 'Year': int(year),
                                                                 'Quarter': quarter,
                                                                 'InsuranceType': item_data.get('name'), # This will be "Insurance"
                                                                 'InsuranceCount': count,
                                                                 'InsuranceAmount': amount
                                                             })
                                                         # Assuming only one TOTAL metric for Insurance per file, break inner loops
                                                         break # Breaks from the instrument loop
                                                 break # Breaks from the item_data (transactionData list) loop

                                    # else: No need for a warning here, as not finding 'Insurance' is expected for some items

                                else:
                                    # This else catches files where the basic structure data.data.transactionData is missing or wrong type
                                    # print(f"Warning: Unexpected data structure or missing 'data.data.transactionData' list in {quarter_path}")
                                    pass # Suppress frequent warnings


                            except json.JSONDecodeError as e:
                                print(f"Error decoding JSON in file {quarter_path}: {e}")
                            except Exception as e:
                                # Catch any other unexpected errors during file processing
                                print(f"An unexpected error occurred processing file {quarter_path}: {e}")

    # print(f"Finished processing for path: {path}. Extracted {len(extracted_data)} rows.") # Optional debug print
    return extracted_data











def process_map_transaction_data(path):
    extracted_data = []
    for state in os.listdir(path):
        state_path = os.path.join(path, state)
        if os.path.isdir(state_path):
            for year in os.listdir(state_path):
                year_path = os.path.join(state_path, year)
                if os.path.isdir(year_path):
                    for quarter_file in os.listdir(year_path):
                        quarter_path = os.path.join(year_path, quarter_file)
                        if quarter_file.endswith('.json'):
                            try:
                                with open(quarter_path, 'r') as json_file:
                                    data = json.load(json_file)
                                    quarter = int(quarter_file.split('.')[0])
                                    if data and 'data' in data and 'hoverDataList' in data['data']:
                                        for district_data in data['data']['hoverDataList']:
                                            district = district_data['name']
                                            # Ensure 'metric' key exists and is a list
                                            if 'metric' in district_data and isinstance(district_data['metric'], list):
                                                 for metric in district_data['metric']:
                                                     if metric.get('type') == 'TOTAL': # Use .get for safe access
                                                         count = metric.get('count')
                                                         amount = metric.get('amount')
                                                         # Only append if count and amount are present
                                                         if count is not None and amount is not None:
                                                            extracted_data.append({
                                                                'State': state,
                                                                'Year': int(year),
                                                                'Quarter': quarter,
                                                                'District': district,
                                                                'TransactionCount': count,
                                                                'TransactionAmount': amount
                                                            })
                                                     # You might need to handle other metric types if applicable
                                            else:
                                                 print(f"Warning: 'metric' key missing or not a list in {quarter_path} for district {district}")
                                    else:
                                         print(f"Warning: Unexpected data structure in {quarter_path}")
                            except Exception as e:
                                print(f"Error processing file {quarter_path}: {e}")
    return extracted_data
'''
def process_map_user_data(path):
    extracted_data = []
    for state in os.listdir(path):
        state_path = os.path.join(path, state)
        if os.path.isdir(state_path):
            for year in os.listdir(state_path):
                year_path = os.path.join(state_path, year)
                if os.path.isdir(year_path):
                    for quarter_file in os.listdir(year_path):
                        quarter_path = os.path.join(year_path, quarter_file)
                        if quarter_file.endswith('.json'):
                            try:
                                with open(quarter_path, 'r') as json_file:
                                    data = json.load(json_file)
                                    quarter = int(quarter_file.split('.')[0])

                                    # Access the 'hoverData' dictionary
                                    hover_data = data.get('data', {}).get('hoverData', {}) # <--- Correctly access 'hoverData'

                                    # Iterate through the districts in the hoverData dictionary
                                    if hover_data: # Check if the dictionary is not empty
                                        for district_name, metrics in hover_data.items(): # <--- Iterate through key-value pairs
                                            # district_name is the key (e.g., "east godavari district")
                                            # metrics is the value (e.g., {"registeredUsers": 454398, "appOpens": 0})

                                            # Extract registeredUsers directly from the metrics dictionary
                                            registered_users = metrics.get('registeredUsers') # <--- Access registeredUsers here

                                            if district_name and registered_users is not None:
                                                extracted_data.append({
                                                    'State': state,
                                                    'Year': int(year),
                                                    'Quarter': quarter,
                                                    'District': district_name, # Use the dictionary key as the district name
                                                    'RegisteredUsers': registered_users
                                                })
                                            # else:
                                                # print(f"Warning: Missing 'name' or 'registeredUsers' in {quarter_path} for entry {district_name}")


                                    else:
                                        print(f"Warning: No 'hoverData' or empty dictionary in file: {quarter_path}")
                            except Exception as e:
                                print(f"Error processing file {quarter_path}: {e}")
    return extracted_data
'''
def process_map_user_data(path):
    """
    Extracts user data (registered users and app opens) for districts from JSON files.
    Includes debug prints to trace execution and data extraction.

    Assumes JSON structure: data -> hoverData -> {district_name} -> {metrics: registeredUsers, appOpens}.
    This structure is based on the provided map user sample JSON.
    """
    extracted_data = []
    print(f"DEBUG: Starting processing for Map User data in path: {path}") # Debug print

    # Check if the base path exists
    if not os.path.exists(path):
        print(f"DEBUG: Error: Base path not found: {path}")
        return extracted_data # Return empty if base path doesn't exist

    # Loop through each state directory
    for state in os.listdir(path):
        state_path = os.path.join(path, state)
        # print(f"DEBUG: Checking state path: {state_path}") # Debug print (can be noisy)

        # Ensure it's a directory before proceeding
        if os.path.isdir(state_path):
            # Loop through each year directory within the state
            for year in os.listdir(state_path):
                year_path = os.path.join(state_path, year)
                # print(f"DEBUG: Checking year path: {year_path}") # Debug print (can be noisy)

                # Ensure it's a directory before proceeding
                if os.path.isdir(year_path):
                    # Loop through each file within the year directory (quarter files)
                    for quarter_file in os.listdir(year_path):
                        quarter_path = os.path.join(year_path, quarter_file)
                        # print(f"DEBUG: Checking file: {quarter_path}") # Debug print (can be noisy)

                        # Process only JSON files
                        if quarter_file.endswith('.json'):
                            try:
                                # Extract the quarter number from the filename (e.g., '1.json' -> 1)
                                # Add a check to ensure the filename is just a number before splitting
                                base_name, ext = os.path.splitext(quarter_file)
                                if base_name.isdigit():
                                    quarter = int(base_name)
                                    year_int = int(year) # Ensure year is an integer
                                else:
                                    print(f"DEBUG: Skipping non-numeric file name: {quarter_file}")
                                    continue # Skip this file if name is not just a digit

                                # Open and load the JSON file
                                with open(quarter_path, 'r') as json_file:
                                    data = json.load(json_file)
                                print(f"DEBUG: Successfully loaded JSON from {quarter_file}") # Debug print

                                # --- Data Extraction Logic for Map User Districts (based on new structure) ---
                                # Navigate to the 'hoverData' dictionary
                                hover_data = data.get('data', {}).get('hoverData')

                                if isinstance(hover_data, dict):
                                    print(f"DEBUG: Found 'hoverData' dictionary ({len(hover_data)} entries) in {quarter_file}") # Debug print
                                    # Iterate through the items in the 'hoverData' dictionary
                                    # Each key is a district name, each value is the metrics dictionary
                                    for district_name, metrics in hover_data.items():
                                        # Ensure metrics is a dictionary
                                        if isinstance(metrics, dict):
                                            # Extracting registered users and app opens
                                            registered_users = metrics.get('registeredUsers')
                                            app_opens = metrics.get('appOpens') # Extract the 'appOpens' value

                                            # Ensure district name and registered users count are present
                                            # app_opens might be None, which is allowed by your schema
                                            if district_name and registered_users is not None:
                                                extracted_data.append({
                                                    'State': state,
                                                    'Year': year_int,
                                                    'Quarter': quarter,
                                                    'District': district_name, # Use the key as the District name
                                                    'RegisteredUsers': registered_users,
                                                    'AppOpens': app_opens # Include the extracted app_opens
                                                })
                                                # print(f"DEBUG: Extracted data for {district_name} in {quarter_file}") # Debug print per entry
                                            else:
                                                print(f"DEBUG: Skipping entry in {quarter_file} due to missing 'name' or 'registeredUsers': {district_name}: {metrics}") # Debug print for skipped entries

                                else:
                                    print(f"DEBUG: 'hoverData' not found or not a dictionary in {quarter_file}. Found type: {type(hover_data)}") # Debug print if dict not found/wrong type


                            except json.JSONDecodeError as e:
                                print(f"DEBUG: Error decoding JSON in file {quarter_path}: {e}")
                            except Exception as e:
                                # Catch any other unexpected errors during file processing
                                print(f"DEBUG: An unexpected error occurred processing file {quarter_path}: {e}")

    print(f"DEBUG: Finished processing for Map User data. Extracted {len(extracted_data)} rows.") # Debug print
    return extracted_data






# --- New Function for Map Insurance Data ---
def process_map_insurance_data(path):
    extracted_data = []
    for state in os.listdir(path):
        state_path = os.path.join(path, state)
        if os.path.isdir(state_path):
            for year in os.listdir(state_path):
                year_path = os.path.join(state_path, year)
                if os.path.isdir(year_path):
                    for quarter_file in os.listdir(year_path):
                        quarter_path = os.path.join(year_path, quarter_file)
                        if quarter_file.endswith('.json'):
                            try:
                                with open(quarter_path, 'r') as json_file:
                                    data = json.load(json_file)
                                    quarter = int(quarter_file.split('.')[0])
                                    # Assuming map insurance data is structured like map transaction data
                                    # Path: data -> hoverDataList -> list of districts
                                    if data and 'data' in data and 'hoverDataList' in data['data'] and isinstance(data['data']['hoverDataList'], list):
                                        for district_data in data['data']['hoverDataList']:
                                            district = district_data.get('name') # Use .get for safe access
                                            # Ensure 'metric' key exists and is a list
                                            if 'metric' in district_data and isinstance(district_data['metric'], list):
                                                 # Assuming 'metric' contains count and amount, potentially with a 'type'
                                                 # Let's extract count and amount directly from the first item in 'metric' list
                                                 # based on observation from similar data structures, or look for a 'TOTAL' type
                                                 # Let's assume it's like map transactions and looks for 'TOTAL' type
                                                for metric in district_data['metric']:
                                                     if metric.get('type') == 'TOTAL': # Check for TOTAL type or adapt based on actual data
                                                        count = metric.get('count')
                                                        amount = metric.get('amount')
                                                        # Only append if district name, count and amount are present
                                                        if district and count is not None and amount is not None:
                                                             extracted_data.append({
                                                                'State': state,
                                                                'Year': int(year),
                                                                'Quarter': quarter,
                                                                'District': district,
                                                                'InsuranceCount': count, # Changed column name
                                                                'InsuranceAmount': amount # Changed column name
                                                             })
                                                             break # Assuming only one relevant metric per district entry

                                            # else:
                                                # print(f"Warning: 'metric' key missing or not a list in {quarter_path} for district entry: {district_data}")


                                    # else:
                                        # print(f"Warning: Unexpected data structure or missing 'hoverDataList' in {quarter_path}")
                                    pass # Suppress frequent warnings

                            except Exception as e:
                                print(f"Error processing file {quarter_path}: {e}")
    return extracted_data

'''
def process_top_transaction_data(path):
    extracted_data = []
    for state in os.listdir(path):
        state_path = os.path.join(path, state)
        if os.path.isdir(state_path):
            for year in os.listdir(state_path):
                year_path = os.path.join(state_path, year)
                if os.path.isdir(year_path):
                    for quarter_file in os.listdir(year_path):
                        quarter_path = os.path.join(year_path, quarter_file)
                        if quarter_file.endswith('.json'):
                            try:
                                with open(quarter_path, 'r') as json_file:
                                    data = json.load(json_file)
                                    quarter = int(quarter_file.split('.')[0])
                                    # Processing top transaction data - Ensure 'pincodes' key exists and is a list
                                    if data and 'data' in data and 'pincodes' in data['data'] and isinstance(data['data']['pincodes'], list):
                                         for entry in data['data']['pincodes']:
                                             district = entry.get('entityName') # Use .get for safe access
                                             metric = entry.get('metric')
                                             if district and metric and isinstance(metric, dict): # Check if metric is a dictionary
                                                 count = metric.get('count')
                                                 amount = metric.get('amount')
                                                 if count is not None and amount is not None:
                                                     extracted_data.append({
                                                         'State': state,
                                                         'Year': int(year),
                                                         'Quarter': quarter,
                                                         'District': district,
                                                         'TransactionCount': count,
                                                         'TransactionAmount': amount
                                                     })
                                             else:
                                                print(f"Warning: Missing 'entityName' or invalid 'metric' in {quarter_path} for entry {entry}")
                                    else:
                                         print(f"Warning: Unexpected data structure or missing 'pincodes' in {quarter_path}")
                            except Exception as e:
                                print(f"Error processing file {quarter_path}: {e}")
    return extracted_data
'''


def process_top_transaction_pincode_data(path):
    extracted_data = []
    # print(f"Starting processing for Top Transaction Pincode data in path: {path}") # Optional debug print

    for state in os.listdir(path):
        state_path = os.path.join(path, state)
        if os.path.isdir(state_path):
            for year in os.listdir(state_path):
                year_path = os.path.join(state_path, year)
                if os.path.isdir(year_path):
                    for quarter_file in os.listdir(year_path):
                        quarter_path = os.path.join(year_path, quarter_file)
                        if quarter_file.endswith('.json'):
                            try:
                                with open(quarter_path, 'r') as json_file:
                                    data = json.load(json_file)
                                quarter = int(quarter_file.split('.')[0])
                                year_int = int(year) # Ensure year is an integer

                                # Navigate to the list containing pincode data
                                # Assuming the path is data -> topTransaction -> pincodes
                                # Or maybe directly data -> pincodes as in your process_top_transaction_data
                                # Let's assume it's within data['data'] and under the key 'pincodes' as suggested by your previous code
                                pincode_list = data.get('data', {}).get('pincodes')

                                if isinstance(pincode_list, list):
                                    for entry in pincode_list:
                                        # Extracting the pincode name and metric
                                        pincode_name = entry.get('entityName') # Assuming 'entityName' holds the pincode
                                        metric_data = entry.get('metric') # Assuming 'metric' is a dictionary with count/amount

                                        if pincode_name and metric_data and isinstance(metric_data, dict):
                                             count = metric_data.get('count')
                                             amount = metric_data.get('amount')

                                             if count is not None and amount is not None:
                                                 extracted_data.append({
                                                     'State': state,
                                                     'Year': year_int,
                                                     'Quarter': quarter,
                                                     'Pincode': int(pincode_name), # Convert pincode to INT as per DB schema
                                                     'TransactionCount': count,
                                                     'TransactionAmount': amount
                                                 })
                                             # else:
                                             #    print(f"Warning: Missing count or amount in metric for pincode {pincode_name} in file {quarter_path}")

                                # else:
                                #     print(f"Warning: 'pincodes' key missing or not a list in file: {quarter_path}")

                            except json.JSONDecodeError as e:
                                print(f"Error decoding JSON in file {quarter_path}: {e}")
                            except ValueError as e:
                                print(f"Error converting pincode to int in file {quarter_path}: {e} - Pincode found: {entry.get('entityName')}") # More specific error for pincode conversion
                            except Exception as e:
                                print(f"An unexpected error occurred processing file {quarter_path}: {e}")

    # print(f"Finished processing for Top Transaction Pincode data. Extracted {len(extracted_data)} rows.") # Optional debug print
    return extracted_data


# --- New Function for Top Transaction District Data ---
def process_top_transaction_district_data(path):
    """
    Extracts transaction data for top districts from JSON files.

    Assumes JSON structure: data -> data -> ['states', 'districts', or 'entities'] -> list of items
    Each item has 'entityName' (district name) and 'metric' (dict with 'count', 'amount').
    """
    extracted_data = []
    # print(f"Starting processing for Top Transaction District data in path: {path}") # Optional debug print

    # Loop through each state directory
    for state in os.listdir(path):
        state_path = os.path.join(path, state)

        # Ensure it's a directory before proceeding
        if os.path.isdir(state_path):
            # Loop through each year directory within the state
            for year in os.listdir(state_path):
                year_path = os.path.join(state_path, year)

                # Ensure it's a directory before proceeding
                if os.path.isdir(year_path):
                    # Loop through each file within the year directory (quarter files)
                    for quarter_file in os.listdir(year_path):
                        quarter_path = os.path.join(year_path, quarter_file)

                        # Process only JSON files
                        if quarter_file.endswith('.json'):
                            try:
                                # Extract the quarter number from the filename (e.g., '1.json' -> 1)
                                quarter = int(quarter_file.split('.')[0])
                                year_int = int(year) # Ensure year is an integer

                                # Open and load the JSON file
                                with open(quarter_path, 'r') as json_file:
                                    data = json.load(json_file)

                                # --- Data Extraction Logic for Top Transaction Districts ---
                                # Look for the list of top entities. Trying common keys.
                                # If the actual JSON structure uses a different key, this needs adjustment.
                                top_entity_list = data.get('data', {}).get('states') # Try 'states' first

                                if not isinstance(top_entity_list, list):
                                     top_entity_list = data.get('data', {}).get('districts') # Then try 'districts'
                                if not isinstance(top_entity_list, list):
                                     top_entity_list = data.get('data', {}).get('entities') # Generic fallback 'entities'


                                if isinstance(top_entity_list, list):
                                    # Iterate through the entries (assumed to be districts)
                                    for entry in top_entity_list:
                                        # Extracting the entity name (district name) and metric data
                                        district_name = entry.get('entityName') # Assuming 'entityName' holds the district name
                                        metric_data = entry.get('metric') # Assuming 'metric' is a dictionary with count/amount

                                        # Ensure district name, metric data, and metric structure are valid
                                        if district_name and metric_data and isinstance(metric_data, dict):
                                            count = metric_data.get('count')
                                            amount = metric_data.get('amount')

                                            # Append the extracted data if count and amount are present
                                            if count is not None and amount is not None:
                                                extracted_data.append({
                                                    'State': state,
                                                    'Year': year_int,
                                                    'Quarter': quarter,
                                                    'District': district_name, # Use entityName as the District name
                                                    'TransactionCount': count,
                                                    'TransactionAmount': amount
                                                })
                                            # else: # Optional warning for missing count/amount in a valid metric entry
                                            # print(f"Warning: Missing count or amount in metric for entity {district_name} in file {quarter_path}")
                                # else: # Optional warning if no suitable list key ('states', 'districts', 'entities') found
                                # print(f"Warning: No suitable list key ('states', 'districts', 'entities') found under 'data' in file: {quarter_path}")


                            except json.JSONDecodeError as e:
                                print(f"Error decoding JSON in file {quarter_path}: {e}")
                            except Exception as e:
                                # Catch any other unexpected errors during file processing
                                print(f"An unexpected error occurred processing file {quarter_path}: {e}")

    # print(f"Finished processing for Top Transaction District data. Extracted {len(extracted_data)} rows.") # Optional debug print
    return extracted_data






def process_top_user_pincode_data(path):
    extracted_data = []
    for state in os.listdir(path):
        state_path = os.path.join(path, state)
        if os.path.isdir(state_path):
            for year in os.listdir(state_path):
                year_path = os.path.join(state_path, year)
                if os.path.isdir(year_path):
                    for quarter_file in os.listdir(year_path):
                        quarter_path = os.path.join(year_path, quarter_file)
                        if quarter_file.endswith('.json'):
                            try:
                                with open(quarter_path, 'r') as json_file:
                                    data = json.load(json_file)
                                    quarter = int(quarter_file.split('.')[0])
                                    # Processing top user data - Ensure 'pincodes' key exists and is a list
                                    if data and 'data' in data and 'pincodes' in data['data'] and isinstance(data['data']['pincodes'], list):
                                        for entry in data['data']['pincodes']:
                                            district = entry.get('name') # Use .get for safe access
                                            registered_users = entry.get('registeredUsers') # Use .get for safe access
                                            if district and registered_users is not None:
                                                extracted_data.append({
                                                    'State': state,
                                                    'Year': int(year),
                                                    'Quarter': quarter,
                                                    'District': district,
                                                    'RegisteredUsers': registered_users
                                                })
                                            else:
                                                print(f"Warning: Missing 'name' or 'registeredUsers' in {quarter_path} for entry {entry}")
                                    else:
                                        print(f"Warning: Unexpected data structure or missing 'pincodes' in {quarter_path}")
                            except Exception as e:
                                print(f"Error processing file {quarter_path}: {e}")
    return extracted_data


# --- Function for Top User District Data (Refined based on sample JSON) ---
def process_top_user_district_data(path):
    """
    Extracts user data (registered users) for top districts from JSON files
    based on the provided sample structure.

    Assumes JSON structure: data -> data -> 'districts' -> list of items
    Each item has 'name' (district name) and 'registeredUsers'.
    """
    extracted_data = []
    # print(f"Starting processing for Top User District data in path: {path}") # Optional debug print

    # Loop through each state directory
    for state in os.listdir(path):
        state_path = os.path.join(path, state)

        # Ensure it's a directory before proceeding
        if os.path.isdir(state_path):
            # Loop through each year directory within the state
            for year in os.listdir(state_path):
                year_path = os.path.join(state_path, year)

                # Ensure it's a directory before proceeding
                if os.path.isdir(year_path):
                    # Loop through each file within the year directory (quarter files)
                    for quarter_file in os.listdir(year_path):
                        quarter_path = os.path.join(year_path, quarter_file)

                        # Process only JSON files
                        if quarter_file.endswith('.json'):
                            try:
                                # Extract the quarter number from the filename (e.g., '1.json' -> 1)
                                quarter = int(quarter_file.split('.')[0])
                                year_int = int(year) # Ensure year is an integer

                                # Open and load the JSON file
                                with open(quarter_path, 'r') as json_file:
                                    data = json.load(json_file)

                                # --- Data Extraction Logic for Top User Districts ---
                                # Access the list of districts using the key 'districts' as per sample
                                district_list = data.get('data', {}).get('districts')

                                if isinstance(district_list, list):
                                    # Iterate through the district entries
                                    for entry in district_list:
                                        # Extracting the district name and registered users count
                                        district_name = entry.get('name') # Using 'name' as the key for district name
                                        registered_users = entry.get('registeredUsers') # Using 'registeredUsers'

                                        # Ensure district name and registered users count are present
                                        if district_name and registered_users is not None:
                                            extracted_data.append({
                                                'State': state,
                                                'Year': year_int,
                                                'Quarter': quarter,
                                                'District': district_name, # Use 'name' as the District name
                                                'RegisteredUsers': registered_users
                                            })
                                        # else: # Optional warning for missing name or registeredUsers in an entry
                                        # print(f"Warning: Missing 'name' or 'registeredUsers' in entry {entry} in file {quarter_path}")

                                # else: # Optional warning if 'districts' list not found
                                # print(f"Warning: 'districts' list not found under 'data' in file: {quarter_path}")


                            except json.JSONDecodeError as e:
                                print(f"Error decoding JSON in file {quarter_path}: {e}")
                            except Exception as e:
                                # Catch any other unexpected errors during file processing
                                print(f"An unexpected error occurred processing file {quarter_path}: {e}")

    # print(f"Finished processing for Top User District data. Extracted {len(extracted_data)} rows.") # Optional debug print
    return extracted_data


# --- New Function for Top Insurance Data ---
def process_top_insurance_data(path):
    extracted_data = []
    for state in os.listdir(path):
        state_path = os.path.join(path, state)
        if os.path.isdir(state_path):
            for year in os.listdir(state_path):
                year_path = os.path.join(state_path, year)
                if os.path.isdir(year_path):
                    for quarter_file in os.listdir(year_path):
                        quarter_path = os.path.join(year_path, quarter_file)
                        if quarter_file.endswith('.json'):
                            try:
                                with open(quarter_path, 'r') as json_file:
                                    data = json.load(json_file)
                                    quarter = int(quarter_file.split('.')[0])
                                    # Assuming top insurance data is structured like top transaction data
                                    # Path: data -> pincodes -> list of top pincodes/districts
                                    if data and 'data' in data and 'pincodes' in data['data'] and isinstance(data['data']['pincodes'], list):
                                        for entry in data['data']['pincodes']:
                                            district_or_pincode = entry.get('entityName') # Use .get for safe access
                                            metric = entry.get('metric') # Should be a dictionary
                                            if district_or_pincode and metric and isinstance(metric, dict):
                                                count = metric.get('count')
                                                amount = metric.get('amount')
                                                if count is not None and amount is not None:
                                                    extracted_data.append({
                                                        'State': state,
                                                        'Year': int(year),
                                                        'Quarter': quarter,
                                                        'Pincode': district_or_pincode, # Renamed column
                                                        'InsuranceCount': count, # Changed column name
                                                        'InsuranceAmount': amount # Changed column name
                                                    })
                                            # else:
                                                # print(f"Warning: Missing 'entityName' or invalid 'metric' in {quarter_path} for entry {entry}")
                                    # else:
                                        # print(f"Warning: Unexpected data structure or missing 'pincodes' list in {quarter_path}")
                                    pass # Suppress frequent warnings
                            except Exception as e:
                                print(f"Error processing file {quarter_path}: {e}")
    return extracted_data












# --- Data Extraction and DataFrame Creation ---

# Define paths to the data directories (ensure these paths are correct relative to your script)
path_to_agg_transaction_json = 'pulse/data/aggregated/transaction/country/india/state'
path_to_agg_user_json = 'pulse/data/aggregated/user/country/india/state'

path_to_agg_insurance_json = 'pulse/data/aggregated/insurance/country/india/state' # New Path

path_to_map_transaction_json = 'pulse/data/map/transaction/hover/country/india/state'
path_to_map_user_json = 'pulse/data/map/user/hover/country/india/state' # Corrected path

path_to_map_insurance_json = 'pulse/data/map/insurance/hover/country/india/state' # New Path

path_to_top_transaction_json = 'pulse/data/top/transaction/country/india/state'
path_to_top_user_json = 'pulse/data/top/user/country/india/state'

path_to_top_insurance_json = 'pulse/data/top/insurance/country/india/state' # New Path


print("Processing aggregated transaction data...")
extracted_agg_transaction_data = process_agg_transaction_data(path_to_agg_transaction_json)
df_agg_transaction = pd.DataFrame(extracted_agg_transaction_data)
print(f"Processed {len(df_agg_transaction)} rows for aggregated transactions.")


print("Processing aggregated user data...")
extracted_agg_user_data = process_agg_user_data(path_to_agg_user_json)
df_agg_user = pd.DataFrame(extracted_agg_user_data)
print(f"Processed {len(df_agg_user)} rows for aggregated users.")

#insurance
print("Processing aggregated insurance data...")
extracted_agg_insurance_data = process_agg_insurance_data(path_to_agg_insurance_json)
df_agg_insurance = pd.DataFrame(extracted_agg_insurance_data)
print(f"Processed {len(df_agg_insurance)} rows for aggregated insurance.")



print("Processing map transaction data...")
extracted_map_transaction_data = process_map_transaction_data(path_to_map_transaction_json)
df_map_transactions = pd.DataFrame(extracted_map_transaction_data)
print(f"Processed {len(df_map_transactions)} rows for map transactions.")

print("Processing map user data...")
extracted_map_user_data = process_map_user_data(path_to_map_user_json) # Use the correct path here
df_map_user = pd.DataFrame(extracted_map_user_data)
print(f"Processed {len(df_map_user)} rows for map users.")

#insurance
print("Processing map insurance data...")
extracted_map_insurance_data = process_map_insurance_data(path_to_map_insurance_json)
df_map_insurance = pd.DataFrame(extracted_map_insurance_data)
print(f"Processed {len(df_map_insurance)} rows for map insurance.")

'''
print("Processing top transaction data...")
extracted_top_transaction_data = process_top_transaction_data(path_to_top_transaction_json)
df_top_transaction = pd.DataFrame(extracted_top_transaction_data)
print(f"Processed {len(df_top_transaction)} rows for top transactions.")
'''
print("Processing top transaction pincode data...")
extracted_top_transaction_pincode_data = process_top_transaction_pincode_data(path_to_top_transaction_json)
df_top_transaction_pincode = pd.DataFrame(extracted_top_transaction_pincode_data)
print(f"Processed {len(df_top_transaction_pincode)} rows for top transaction pincodes.")


print("Processing top transaction district data...")
extracted_top_transaction_district_data = process_top_transaction_district_data(path_to_top_transaction_json) # Use the same path as top pincodes
df_top_transaction_district = pd.DataFrame(extracted_top_transaction_district_data)
print(f"Processed {len(df_top_transaction_district)} rows for top transaction districts.")


print("Processing top user pincode data...")
extracted_top_user_pincode_data = process_top_user_pincode_data(path_to_top_user_json)
df_top_user_pincode = pd.DataFrame(extracted_top_user_pincode_data)
print(f"Processed {len(df_top_user_pincode)} rows for top user pincode.")

print("Processing top user district data...")
extracted_top_user_district_data = process_top_user_district_data(path_to_top_user_json)
df_top_user_district = pd.DataFrame(extracted_top_user_district_data)
print(f"Processed {len(df_top_user_district)} rows for top user pincode.")

#insurance
print("Processing top insurance data...")
extracted_top_insurance_data = process_top_insurance_data(path_to_top_insurance_json)
df_top_insurance = pd.DataFrame(extracted_top_insurance_data)
print(f"Processed {len(df_top_insurance)} rows for top insurance.")

print("--- Data Extraction Completed ---")
# --- Database Insertion Function ---

def insert_dataframe_to_sql(df, table_name, replace_table=False): # Removed connection_details parameter
    try:
        # Create an SQLAlchemy engine using imported credentials and encoded password
        # Ensure DB_PORT is included if it's not the default 3306
        engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{ENCODED_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_DATABASE}")

        # Define a mapping from Pandas dtype to SQL dtype
        dtype_mapping = {
            'object': types.VARCHAR(255),
            'int64': types.BIGINT,
            'float64': types.FLOAT,
            'datetime64[ns]': types.DateTime,
            # Add other dtype mappings as needed
        }
        # Map the DataFrame dtypes using the mapping
        sql_dtypes = {}
        for col in df.columns:
             pandas_dtype = str(df[col].dtype)
             if pandas_dtype in dtype_mapping:
                 sql_dtypes[col] = dtype_mapping[pandas_dtype]
             else:
                 # Default type or raise an error if an unexpected dtype is found
                 # print(f"Warning: No specific SQL type mapping for dtype {pandas_dtype} in column {col}. Using VARCHAR.")
                 sql_dtypes[col] = types.VARCHAR(255) # Default to VARCHAR for unsupported types


        # Replace or append to the table as specified
        if_exists_action = 'replace' if replace_table else 'append'

        print(f"Inserting data into table: {table_name} (if_exists='{if_exists_action}')...")
        # Use to_sql to insert the data
        df.to_sql(name=table_name, con=engine, if_exists=if_exists_action, index=False, dtype=sql_dtypes)
        print(f"Successfully inserted data into table: {table_name}")

    except Exception as e:
        # Print a more specific error if possible, but the general exception catch is fine
        print(f"An error occurred while inserting into {table_name}: {e}")
    finally:
        # Close the SQLAlchemy engine
        if 'engine' in locals() and engine:
             engine.dispose()


# --- Insert DataFrames into SQL ---

print("\n--- Starting Database Insertion ---")


# Call insert function using imported details directly
insert_dataframe_to_sql(df_agg_transaction, 'aggregated_transaction', replace_table=True)
insert_dataframe_to_sql(df_agg_user, 'aggregated_user', replace_table=True)
insert_dataframe_to_sql(df_agg_insurance, 'aggregated_insurance') # Insert new table

insert_dataframe_to_sql(df_map_transactions, 'map_transactions', replace_table=True)
insert_dataframe_to_sql(df_map_user, 'map_users', replace_table=True)
insert_dataframe_to_sql(df_map_insurance, 'map_insurance', replace_table=True) # Insert new table

#insert_dataframe_to_sql(df_top_transaction, 'top_transactions', replace_table=True)
insert_dataframe_to_sql(df_top_transaction_pincode, 'top_transaction_pincode', replace_table=True) # Insert into the pincode table
insert_dataframe_to_sql(df_top_transaction_district, 'top_transaction_district', replace_table=True) # Insert into district table
insert_dataframe_to_sql(df_top_user_pincode, 'top_user_pincode', replace_table=True)
insert_dataframe_to_sql(df_top_user_district, 'top_user_district', replace_table=True)

insert_dataframe_to_sql(df_top_insurance, 'top_insurance_pincode', replace_table=True) # Insert new table


print("\n--- Database Insertion Process Completed ---")