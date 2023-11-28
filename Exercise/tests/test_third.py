import pandas as pd
import pytest


def test_dataframes_match():
    # Set display options to show all rows and columns
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)

    # Call the function with the provided directory path
    expected_df = pd.read_csv('resources/output/target.csv')
    expected_df = expected_df[~expected_df['EGORRES'].isin(['YES', 'NO'])]  # Additional Copied Row
    expected_df = expected_df.drop(columns=['Unnamed: 0', 'EGSEQ', 'EGDY'])

    output_df = pd.read_csv('final_output.csv')
    output_df = output_df.drop(columns=['Unnamed: 0','EGSEQ', 'EGDY'])

    expected_df = expected_df.sort_values(by=['USUBJID', 'EGDTC'], ascending=False)
    output_df = output_df.sort_values(by=['USUBJID', 'EGDTC'], ascending=False)

    df_diff = pd.concat([expected_df, output_df]).drop_duplicates(keep=False)

    # Assert that the DataFrames are equal, which means there should be no differences
    assert df_diff.empty, f"DataFrames do not match. Rows not matching: {df_diff.shape}"


# If you want to run this script independently, you can use the following:
if __name__ == '__main__':
    pytest.main([__file__])
