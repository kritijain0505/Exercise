import pandas as pd
import hashlib
import os
import sys
import psycopg2
from psycopg2 import sql

# Set display options to print the complete DataFrame
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)


def table_exists(conn, table_name):
    with conn.cursor() as cur:
        # Check if the table exists
        table_exists_query = sql.SQL(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)").format(
            sql.Identifier(table_name))
        cur.execute(table_exists_query, (table_name,))
        return cur.fetchone()[0]


def read_data(conn, schema_name, table_name, columns):
    with conn.cursor() as cur:
        select_query = "SELECT " + ", ".join(['"' + col + '"' for col in columns]) + f" FROM {schema_name}.{table_name}"
        cur.execute(select_query)
        rows = cur.fetchall()
        column_names = [desc[0] for desc in cur.description]
        df = pd.DataFrame(rows, columns=column_names)
        return df


def generate_hash(data):
    hash_object = hashlib.sha256(data.encode())
    return hash_object.hexdigest()


def create_table(cur, dataframe, table_name):
    columns = ', '.join([f'"{col}" VARCHAR' for col in dataframe.columns])
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
    cur.execute(create_table_query)


def write_to_postgres(conn, dataframe, table_name):
    try:
        with conn.cursor() as cur:
            create_table(cur, dataframe, table_name)
            dataframe.to_csv('temp.csv', index=False)
            with open('temp.csv', 'r') as f:
                cur.copy_expert(f"COPY {table_name} FROM STDIN CSV HEADER", f)
            conn.commit()
    except Exception as e:
        print(f"Error: {e}")
        if cur:
            cur.close()


# Function to delete records from the PostgreSQL table based on the provided DataFrame
def delete_records(conn, table_name, df, column_name):

    with conn.cursor() as cursor:
         for index, row in df.iterrows():
             hash_value = row[column_name]
             delete_query = f"DELETE FROM {table_name} WHERE {column_name} = %s"
             cursor.execute(delete_query, (hash_value,))
         conn.commit()
         cursor.close()


def update_records(conn, table_name, condition_column_name, condition_value_dataframe, update_column, update_value):
    with conn.cursor() as cursor:
        update_query = sql.SQL("""
            UPDATE {table}
            SET {update_column} = %s
            WHERE {condition_column} IN %s
        """).format(
            table=sql.Identifier(table_name),
            update_column=sql.Identifier(update_column),
            condition_column=sql.Identifier(condition_column_name)
        )
        condition_values = tuple(condition_value_dataframe[condition_column_name].tolist())
        cursor.execute(update_query, (update_value, condition_values))
        conn.commit()
        cursor.close()


def compare_dataframes_sql(current_df, previous_df, primary_keys, db_url):
    conn = psycopg2.connect(db_url)

    # Perform left join to identify inserted and updated rows
    merged_df = pd.merge(current_df, previous_df, on=primary_keys, how='left', suffixes=('_current', '_previous'))

    # Identify inserted rows (present in current but not in previous)
    inserted_rows = merged_df[merged_df['hash_primary_columns_previous'].isnull()]

    # Identify deleted rows (present in previous but not in current)
    deleted_rows = previous_df[~previous_df['hash_primary_columns'].isin(current_df['hash_primary_columns'])]

    # Identify updated rows (present in both current and previous with different hash values for non-primary columns)
    updated_rows = merged_df[(~merged_df['hash_primary_columns_previous'].isnull()) &
                             (merged_df['hash_non_primary_columns_current'] != merged_df[
                                 'hash_non_primary_columns_previous'])]

    if not inserted_rows.empty:
        # Get only the columns from current_df and suffix them
        new_columns = [col for col in inserted_rows.columns if '_previous' not in col]
        output_df = inserted_rows[new_columns].copy()
        output_df.columns = output_df.columns.str.rstrip('_current')
        output_df['IUD'] = 'I'
        write_to_postgres(conn, output_df, table_name=table_name)

    if not deleted_rows.empty:
        del_df = deleted_rows[['hash_primary_columns']].copy()
        update_records(conn, table_name, 'hash_primary_columns', del_df, 'IUD', 'D')

    if not updated_rows.empty:
        new_columns = [col for col in updated_rows.columns if '_previous' not in col]
        output_df = updated_rows[new_columns].copy()
        output_df.columns = output_df.columns.str.rstrip('_current')
        output_df['IUD'] = 'U'

        del_df = output_df[['hash_primary_columns']].copy()
        delete_records(conn, table_name, del_df, 'hash_primary_columns')
        write_to_postgres(conn, output_df, table_name=table_name)

if __name__ == "__main__":

    # First Load - resources/input_second/first_load.csv Field1,Field2 public cdc
    # Second Load - resources/input_second/second_load.csv Field1,Field2 public cdc
    if len(sys.argv) != 5:
        print("Usage: python test_first.py <csv_path> <primary_keys> <schema_name> <table_name>")
        sys.exit(1)

    database_url = os.environ.get('DB_URL', 'postgresql://postgres:123@localhost:5432/my_database')
    csv_path = sys.argv[1]
    primary_keys = sys.argv[2].split(',')
    schema_name = sys.argv[3]
    table_name = sys.argv[4]

    input_df = pd.read_csv(csv_path, dtype='object')

    input_df['hash_primary_columns'] = input_df[primary_keys].astype(str).apply(''.join, axis=1).apply(generate_hash)
    input_df['hash_non_primary_columns'] = input_df.drop(columns='hash_primary_columns').astype(str).apply(''.join, axis=1).apply(generate_hash)

    # Connect to the database
    with psycopg2.connect(database_url) as conn:

        # Check if the table exists
        if table_exists(conn, table_name):
            hist_df = read_data(conn, schema_name, table_name, primary_keys + ["hash_primary_columns", "hash_non_primary_columns"])
            compare_dataframes_sql(input_df, hist_df, primary_keys, database_url)
        else:
            input_df['IUD'] = 'I'
            write_to_postgres(conn, input_df, table_name)
