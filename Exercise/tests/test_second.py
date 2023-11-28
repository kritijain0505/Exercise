import unittest
import os
import pandas as pd

from second import CSVToPostgres


class TestCSVToPostgres(unittest.TestCase):
	def setUp(self):
		self.database_url = os.environ.get('DB_URL', 'postgresql://postgres:123@localhost:5432/my_database')
		self.csv_paths = [
			'resources/input_data/ecg1.csv',
			'resources/input_data/ecg2.csv',
			'resources/input_data/ecg3.csv'
		]

	def test_process_csv_files(self):
		csv_to_postgres = CSVToPostgres(self.csv_paths, self.database_url)
		results = csv_to_postgres.process_csv_files()

		# Check if the output is a list of tuples, each containing a DataFrame and a table name
		self.assertIsInstance(results, list)
		for result in results:
			self.assertIsInstance(result, tuple)
			self.assertIsInstance(result[0], pd.DataFrame)
			self.assertIsInstance(result[1], str)

	def test_read_csv_to_dataframe(self):
		csv_to_postgres = CSVToPostgres(self.csv_paths, self.database_url)
		dataframe = csv_to_postgres.read_csv_to_dataframe(self.csv_paths[0])

		# Check if the result is a DataFrame
		self.assertIsInstance(dataframe, pd.DataFrame)

	def test_create_table(self):
		csv_to_postgres = CSVToPostgres(self.csv_paths, self.database_url)
		dataframe = pd.DataFrame({'A': [1, 2], 'B': ['a', 'b']})

		# Create an in-memory SQLite database for testing
		test_db_url = 'sqlite:///:memory:'
		with self.assertRaises(Exception):
			# Expecting an exception because SQLite doesn't support creating tables with VARCHAR directly
			csv_to_postgres.create_table(None, dataframe, 'test_table', test_db_url)

	def tearDown(self):
		# Clean up any temporary files created during testing
		if os.path.exists('temp.csv'):
			os.remove('temp.csv')


if __name__ == '__main__':
	unittest.main()
