import unittest
import os
import sqlite3
import pandas as pd


class DataProcessing(unittest.TestCase):

    def setUp(self):
        print("Setting up the environment...")
        try:
            # Set up SQLite databases
            self.db_path = '../data'
            self.conn = sqlite3.connect(self.db_path)
            self.query1 = f"SELECT * FROM population_table;"
            self.population_df = pd.read_sql_query(self.query1, self.conn)

            self.query2 = f"SELECT * FROM parking-garages_table;"
            self.parking_df = pd.read_sql_query(self.query2, self.conn)
        except Exception as e:
            self.fail(f"Failed to set up the environment: {e}")

    def test_population_database(self):
        print("Running Munich_population_database...")
        try:
            # Test if the population_table table exists in the database
            cursor = self.conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            table_names = [table[0] for table in tables]
            self.assertIn('population_table', table_names)
            print("Test passed: population_table table exists in the database.")
        except Exception as e:
            self.fail(f"Test failed: {e}")

    def test_parking_database(self):
        print("Running parking_garages database...")
        try:
            # Test if the parking-garages table exists in the database
            cursor = self.conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            table_names = [table[0] for table in tables]
            self.assertIn('parking-garages_table', table_names)
            print("Test passed: parking-garages_table table exists in the database.")
        except Exception as e:
            self.fail(f"Test failed: {e}")

    def test_population_dataframe(self):
        print("Running test_population_dataframe...")
        try:
            # Test if the population_df DataFrame is not empty
            self.assertFalse(self.population_df.empty)
            print("Test passed: population_df DataFrame is not empty.")
        except Exception as e:
            self.fail(f"Test failed: {e}")

    def test_parking_dataframe(self):
        print("Running test_parking_dataframe...")
        try:
            # Test if the airbnb_df DataFrame is not empty
            self.assertFalse(self.parking_df.empty)
            print("Test passed: parking_df DataFrame is not empty.")
        except Exception as e:
            self.fail(f"Test failed: {e}")


if __name__ == '__main__':
    unittest.main()
