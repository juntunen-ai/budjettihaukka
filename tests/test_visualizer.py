from langgraph_components.visualizer import visualize_data
import unittest

class TestVisualizer(unittest.TestCase):
    def test_visualize_data(self):
        data = "Sample data"
        result = visualize_data(data)
        self.assertIn("Visualizing", result)
        self.assertEqual(result, f"Visualizing: {data}")

if __name__ == "__main__":
    unittest.main()