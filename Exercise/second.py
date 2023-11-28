import os

import pandas as pd
import psycopg2


class CSVToPostgres:
    def __init__(self, csv_paths, db_url):
        self.csv_paths = csv_paths
        self.db_url = db_url

    def read_csv_to_dataframe(self, csv_path):
        return pd.read_csv(csv_path)

    def write_to_postgres(self, dataframe, table_name):
        try:
            with psycopg2.connect(self.db_url) as conn:
                with conn.cursor() as cur:
                    self.create_table(cur, dataframe, table_name)
                    print("DataFrame Size (Number of Rows, Number of Columns):", dataframe.shape)
                    dataframe.to_csv('temp.csv', index=False)
                    with open('temp.csv', 'r') as f:
                        cur.copy_expert(f"COPY {table_name} FROM STDIN CSV HEADER", f)
                    conn.commit()
        except Exception as e:
            print(f"Error: {e}")
            if cur:
                cur.close()
            if conn:
                conn.close()

    def create_table(self, cur, dataframe, table_name):
        columns = ', '.join([f'"{col}" VARCHAR' for col in dataframe.columns])
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
        cur.execute(create_table_query)

    def process_csv_files(self):
        results = []
        for csv_path in self.csv_paths:
            dataframe = self.read_csv_to_dataframe(csv_path)
            table_name = csv_path.split("/")[-1].split(".")[0]
            self.write_to_postgres(dataframe, table_name)
            results.append((dataframe, table_name))
        return results


if __name__ == "__main__":
    database_url = os.environ.get('DB_URL', 'postgresql://postgres:123@localhost:5432/my_database')

    csv_paths = [
        'resources/input_data/ecg1.csv',
        'resources/input_data/ecg2.csv',
        'resources/input_data/ecg3.csv'
    ]

    csv_to_postgres = CSVToPostgres(csv_paths, database_url)
    csv_to_postgres.process_csv_files()
