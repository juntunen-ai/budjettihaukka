from langgraph_components.data_fetcher import fetch_data
import unittest

class TestDataFetcher(unittest.TestCase):
    def test_fetch_data(self):
        result = fetch_data()
        self.assertIn("data", result)
        self.assertEqual(result["data"], "Sample data fetched")

if __name__ == "__main__":
    unittest.main()