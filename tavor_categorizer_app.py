#!/usr/bin/env python3

import pandas as pd
import re

def load_datasets(import_file, tavor_file):
    """
    Loads the import and tavor CSV files into pandas DataFrames.
    Skips corrupted lines, cleans whitespaces
    """
    print("Loading data...")
    import_df = pd.read_csv(import_file)
    tavor_df = pd.read_csv(tavor_file, on_bad_lines='skip', dtype=str)

    # Strip hidden spaces from description columns to ensure clean text matching
    import_df['Item Description'] = import_df['Item Description'].astype(str).str.strip()
    tavor_df['Item Full Name English'] = tavor_df['Item Full Name English'].astype(str).str.strip()

    return import_df, tavor_df

def prep_tavor_data(tavor_df):
    """
    Cleans and standardizes the Tavor dataset columns.
    """
    # Convert sizes to numbers then back to strings to strip out trailing '.0'
    tavor_df['T_Thread'] = pd.to_numeric(tavor_df['Metric thread M'], errors='coerce').astype(float).astype(str).str.replace(r'\.0$', '', regex=True)
    tavor_df['T_Length'] = pd.to_numeric(tavor_df['Length'], errors='coerce').astype(float).astype(str).str.replace(r'\.0$', '', regex=True)

    # Standardize names and codes
    tavor_df['T_Std_Name'] = tavor_df['Standard Name'].astype(str).str.strip().str.upper()
    tavor_df['T_Std_Code'] = tavor_df['Standard Code'].astype(str).str.strip()
    tavor_df['T_Material'] = tavor_df['Material'].astype(str).str.strip()

    # Standardize coatings (Zn becomes ZNW, empty becomes BLK)
    tavor_df['T_Coating'] = tavor_df['Coating'].fillna('BLK').replace({'Zn': 'ZNW', 'nan': 'BLK'})

    return tavor_df

def parse_description(desc):
    """
    Step 3A: A helper function that uses Regular Expressions (regex)
    to extract specific features from a raw English description string.
    """
    desc_str = str(desc).strip()

    # 1. Extract Thread/Diameter and Length
    # This perfectly captures "M12x60" from "DIA16 -M12x60" by looking for the "X"
    dim_match = re.search(r'(?:M|m|DIA|dia)?\s*(\d+(?:\.\d+)?)\s*[xX*]\s*(\d+(?:\.\d+)?)', desc_str)
    thread = str(float(dim_match.group(1))).rstrip('0').rstrip('.') if dim_match else None
    length = str(float(dim_match.group(2))).rstrip('0').rstrip('.') if dim_match else None

    # 2. Extract Coating (BLK or ZNW)
    coating_match = re.search(r'\b(BLK|ZNW)\b', desc_str, re.IGNORECASE)
    coating = coating_match.group(1).upper() if coating_match else None

    # 3. Extract Material Class (e.g., 8.8, 10.9) - ignores leading zeros like "08.8"
    mat_match = re.search(r'\b0?(4\.6|5\.6|8\.8|10\.9|12\.9|A2|A4)\b', desc_str)
    material = mat_match.group(1) if mat_match else None

    return thread, length, coating, material

def prep_import_data(import_df):
    """
    Step 3: Prepares the import dataset by breaking its descriptions down
    into isolated feature columns using our parse_description function.
    """
    # Split the standard column (e.g., 'DIN 609' -> Name: 'DIN', Code: '609')
    import_df[['I_Std_Name', 'I_Std_Code']] = import_df['Ref Standard'].astype(str).str.extract(r'^([A-Za-z]+)\s+(.+)$')
    import_df['I_Std_Name'] = import_df['I_Std_Name'].str.upper()
    import_df['I_Std_Code'] = import_df['I_Std_Code'].str.strip()

    # Apply our parsing function to every row's description
    extracted_features = import_df['Item Description'].apply(parse_description)

    # Assign the extracted tuple data back to dedicated columns
    import_df['I_Thread'] = [feat[0] for feat in extracted_features]
    import_df['I_Length'] = [feat[1] for feat in extracted_features]
    import_df['I_Coating'] = [feat[2] for feat in extracted_features]
    import_df['I_Mat_Class'] = [feat[3] for feat in extracted_features]

    return import_df

def find_best_match(row, tavor_df):
    """
    Step 4: The core logic. Searches the Tavor dataset for a match
    using exact text first. If that fails, it falls back to feature matching.
    """
    # Strategy A: Exact English Text Match
    desc_en = row['Item Description']
    exact_match = tavor_df[tavor_df['Item Full Name English'] == desc_en]

    if not exact_match.empty:
        raw_id = str(exact_match.iloc[0]['Article code'])
        # Return the integer ID (split removes any '.0' artifacts) and description
        return raw_id.split('.')[0], exact_match.iloc[0]['Item Full Name']

    # Strategy B: Feature-Based Match
    std_name = row['I_Std_Name']
    std_code = row['I_Std_Code']
    th = row['I_Thread']
    ln = row['I_Length']
    coat = row['I_Coating']
    mat = row['I_Mat_Class']

    # Ensure we actually have standard info and dimensions before querying
    if pd.notna(std_name) and pd.notna(std_code) and pd.notna(th) and pd.notna(ln):
        # Build the search filter (mask)
        mask = (
            (tavor_df['T_Std_Name'] == std_name) &
            (tavor_df['T_Std_Code'] == std_code) &
            (tavor_df['T_Thread'] == th) &
            (tavor_df['T_Length'] == ln)
        )

        # Narrow down the search by coating if we found one
        if pd.notna(coat):
            mask = mask & (tavor_df['T_Coating'] == coat)

        # Narrow down the search by material class if we found one
        if pd.notna(mat):
            mask = mask & (tavor_df['T_Material'] == mat)

        # Execute the search
        feature_match = tavor_df[mask]

        if not feature_match.empty:
            raw_id = str(feature_match.iloc[0]['Article code'])
            return raw_id.split('.')[0], feature_match.iloc[0]['Item Full Name']

    # Return blanks if absolutely no match was found
    return None, None

def format_final_dataframe(import_df, final_rows):
    """
    Step 5: Compiles the matched rows back into a DataFrame,
    reorders the columns for clarity, and deletes our temporary extraction columns.
    """
    merged_df = pd.DataFrame(final_rows)

    # Get all column names and remove the temporary ones we made for matching
    cols = merged_df.columns.tolist()
    drop_cols = ['I_Std_Name', 'I_Std_Code', 'I_Thread', 'I_Length', 'I_Coating', 'I_Mat_Class']
    for c in drop_cols:
        if c in cols:
            cols.remove(c)

    # Rearrange columns to put the Tavor ID and Description at the front
    cols.insert(1, cols.pop(cols.index('tavor_id')))
    cols.insert(2, cols.pop(cols.index('tavor_description')))
    merged_df = merged_df[cols]

    # Convert tavor_id to pure integers so Excel treats them correctly
    merged_df['tavor_id'] = pd.to_numeric(merged_df['tavor_id'], errors='coerce')

    return merged_df

def save_to_excel(df, output_filename):
    """
    Step 6: Saves the DataFrame to an Excel file using xlsxwriter,
    applying a custom formatting rule to force IDs to display 6 digits.
    """
    print("Saving to Excel...")

    with pd.ExcelWriter(output_filename, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')

        workbook = writer.book
        worksheet = writer.sheets['Sheet1']

        # '000000' is Excel's command to zero-pad a pure integer up to 6 places
        padded_int_format = workbook.add_format({'num_format': '000000'})

        # Apply this format to Column B ('tavor_id' column)
        worksheet.set_column('B:B', 12, padded_int_format)

    matched_count = df['tavor_id'].notna().sum()
    print(f"Merge complete! Successfully matched {matched_count} out of {len(df)} items.")
    print(f"File saved as '{output_filename}'")

def main():
    """The master function that orchestrates all the steps above."""

    import_df, tavor_df = load_datasets('import.csv', 'tavor.csv')

    tavor_df = prep_tavor_data(tavor_df)
    import_df = prep_import_data(import_df)

    print("Matching items...")
    final_rows = []

    # Iterate through every item in import.csv
    for idx, row in import_df.iterrows():
        # Send the row to our matching engine
        matched_id, matched_desc = find_best_match(row, tavor_df)

        # Convert the row to a dictionary so we can easily add our new data
        row_dict = row.to_dict()
        row_dict['tavor_id'] = matched_id if matched_id != "nan" else None
        row_dict['tavor_description'] = matched_desc if matched_desc else ""

        final_rows.append(row_dict)

    merged_df = format_final_dataframe(import_df, final_rows)

    save_to_excel(merged_df, 'import_updated.xlsx')

if __name__ == "__main__":
    main()
