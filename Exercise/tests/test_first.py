import unittest
from unittest.mock import MagicMock
import pandas as pd
import psycopg2
from first import (
    table_exists,
    read_data,
    generate_hash,
    create_table,
    write_to_postgres,
    delete_records,
    update_records,
    compare_dataframes_sql,
)

class TestFirstFunctions(unittest.TestCase):

    def setUp(self):
        # Set up a mock connection for testing
        self.conn = MagicMock(spec=psycopg2.extensions.connection)

    def test_table_exists(self):
        # Mock the cursor and its execute method
        mock_cursor = self.conn.cursor.return_value
        mock_cursor.fetchone.return_value = (True,)  # Table exists

        # Test table_exists function
        result = table_exists(self.conn, 'test_table')  # Update to use consistent table name

        # Assertions
        self.assertTrue(result)
        mock_cursor.execute.assert_called_once()

    def test_read_data(self):
        # Mock the cursor and its execute method
        mock_cursor = self.conn.cursor.return_value
        mock_cursor.fetchall.return_value = [(1, 'data1'), (2, 'data2')]
        mock_cursor.description = [('col1',), ('col2',)]

        # Test read_data function
        result = read_data(self.conn, 'public', 'test_table', ['col1', 'col2'])  # Update to use consistent table name and column names

        # Assertions
        expected_df = pd.DataFrame([(1, 'data1'), (2, 'data2')], columns=['col1', 'col2'])
        print("Result DataFrame:")
        print(result)
        print("Expected DataFrame:")
        print(expected_df)
        pd.testing.assert_frame_equal(result, expected_df)

if __name__ == '__main__':
    unittest.main()
