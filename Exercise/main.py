import os
import pandas as pd
import sys

# Set display options to show all rows and columns
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

EGORRESU_MAPPING = {
    'Ventricular Rate': 'BPM',
    'PR Interval': 'msec',
    'QRS Interval': 'msec',
    'QT Interval': 'msec',
    'QTc Interval Calc Bazett': 'msec',
    'QTc Interval Calc Fridericia': 'msec'
}

EGSTRESU_MAPPING = {
    'Ventricular Rate': 'VRMEAN',
    'PR Interval': 'PRMEAN',
    'QRS Interval': 'QRSDUR',
    'QT Interval': 'QTMEAN',
    'QTc Interval Calc Bazett': 'QTCBMEAN',
    'QTc Interval Calc Fridericia': 'QTCFMEAN'
}

EGTESTCD_MAPPING = {
    'Ventricular Rate': 'Summary (Mean) Ventricular Rate',
    'PR Interval': 'Summary (Mean) PR Duration',
    'QRS Interval': 'Summary (Mean) QRS Duration',
    'QT Interval': 'Summary (Mean) QT Duration',
    'QTc Interval Calc Bazett': 'QTcB - Bazetts Correction Formula (Mean)',
    'QTc Interval Calc Fridericia': 'QTcF-Fridericias Correction Formul(Mean)'
}

def read_csv_files_in_directory(directory_path):
    # Get a list of all files in the directory
    files = os.listdir(directory_path)

    # Filter only CSV files
    csv_files = [file for file in files if file.endswith('.csv')]

    # Initialize an empty list to store DataFrames
    data_frames = []

    # Iterate through each CSV file and read it into a DataFrame
    for csv_file in csv_files:
        file_path = os.path.join(directory_path, csv_file)
        df = pd.read_csv(file_path)

        # Append each DataFrame to the list
        data_frames.append(df)

    # Concatenate all DataFrames along the index (rows)
    combined_data = pd.concat(data_frames, ignore_index=True)

    return combined_data


# Function to split rows based on specified columns and add a categorical column
def split_row(row, columns_to_split):
    # Check if any value in columns_to_split is NaN
    if row[columns_to_split].notna().all():
        id_columns = [col for col in row.index if col not in columns_to_split]
        new_rows = pd.DataFrame({
            **{col: [row[col]] * len(columns_to_split) for col in id_columns},
            'column_name': columns_to_split,
            'value': [row[col] for col in columns_to_split]
        })
        return new_rows
    else:
        # Return an empty DataFrame if any value in columns_to_split is NaN
        return pd.DataFrame()


if __name__ == "__main__":
    # Check if the directory path is provided as a command-line argument
    if len(sys.argv) != 2:
        print("Usage: python main.py <directory_path>")
        sys.exit(1)

    # Get the directory path from the command line
    directory_path = sys.argv[1]

    # Call the function with the provided directory path
    combined_data = read_csv_files_in_directory(directory_path)

    # Extract the desired column into a new DataFrame
    desired_columns = ['Clinical Trial Number', 'Month of ECG', 'Day of ECG', 'Year of ECG', 'Ventricular Rate',
                       'PR Interval', 'QRS Interval', 'QT Interval', 'QTc Interval Calc Bazett',
                       'QTc Interval Calc Fridericia', 'Subject Number', 'Rel day of ECG to Start of Trt', 'Visit']
    output_data = combined_data[desired_columns].copy()
    output_data['DOMAIN'] = 'EG'
    output_data['EGCAT'] = 'Measurement'
    output_data['EGEVAL'] = 'INVESTIGATOR'
    output_data['EGMETHOD'] = '12 LEAD STANDARD'
    output_data['EGPOS'] = 'SUPINE'
    output_data['EGSTAT'] = ''
    output_data['EGDY'] = output_data['Rel day of ECG to Start of Trt']
    output_data['EGDTC'] = output_data['Month of ECG'].astype(str) + '/' + output_data['Day of ECG'].astype(str) + '/' + output_data['Year of ECG'].astype(str)
    output_data = output_data.drop(columns=['Year of ECG', 'Month of ECG', 'Day of ECG'])
    output_data['USUBJID'] = output_data['Subject Number'].astype(str) + '||' + output_data['Clinical Trial Number'].astype(str)
    output_data = output_data.rename(columns={'Clinical Trial Number': 'STUDYID'})


    # Apply the function to split dynamically at each row and concatenate the results
    result_sub_1 = pd.concat(output_data.apply(lambda row: split_row(row, ['Ventricular Rate', 'PR Interval']), axis=1).tolist(), ignore_index=True)
    result_sub_2 = pd.concat(output_data.apply(lambda row: split_row(row, ['QRS Interval', 'QT Interval']), axis=1).tolist(),ignore_index=True)
    result_sub_3 = pd.concat(output_data.apply(lambda row: split_row(row, ['QTc Interval Calc Bazett', 'QTc Interval Calc Fridericia']),axis=1).tolist(), ignore_index=True)

    # Save the combined data to a CSV file
    final_output = pd.concat([result_sub_1, result_sub_2, result_sub_3])
    final_output.rename(columns={'value': 'EGORRES'}, inplace=True)
    final_output['EGSTRESN'] = final_output['EGORRES']
    final_output['EGSTRRESC'] = final_output['EGORRES']
    final_output['EGORRESU'] = final_output['column_name'].apply(lambda x: EGORRESU_MAPPING.get(x))
    final_output['EGSTRESU'] = final_output['column_name'].apply(lambda x: EGSTRESU_MAPPING.get(x))
    final_output['EGTEST'] = final_output['EGSTRESU']
    final_output['VISIT'] = final_output['Visit']
    final_output['VISITDY'] = final_output['Visit']
    final_output['VISITNUM'] = final_output['Visit']

    final_output['EGTESTCD'] = final_output['column_name'].apply(lambda x: EGTESTCD_MAPPING.get(x))
    final_output = final_output.sort_values(by=['USUBJID', 'EGDTC'], ascending=False)
    final_output['EGSEQ'] = final_output.groupby(['EGDTC']).cumcount() + 1
    col_order = ['DOMAIN', 'EGCAT', 'EGDTC', 'EGDY', 'EGEVAL', 'EGMETHOD', 'EGORRES', 'EGORRESU', 'EGPOS', 'EGSEQ', 'EGSTAT', 'EGSTRESN', 'EGSTRESU', 'EGSTRRESC', 'EGTEST', 'EGTESTCD', 'STUDYID', 'USUBJID', 'VISIT', 'VISITDY', 'VISITNUM']

    # Filter the column names that are present in the DataFrame
    final_output = final_output[[col for col in col_order if col in final_output.columns]]
    final_output.insert(0, '', range(1, 1 + len(final_output)))
    final_output.to_csv('final_output.csv', index=False)