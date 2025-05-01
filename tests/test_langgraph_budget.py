from langgraph_components.budget_analyzer import create_budget_analyzer
import unittest

class TestBudgetAnalyzer(unittest.TestCase):
    def test_budget_analysis(self):
        analyzer = create_budget_analyzer()
        result = analyzer.invoke({"budget_data": {"item1": 100, "item2": 200}})
        self.assertIn("analysis", result)
        # Lisää tarkempia assertioita

if __name__ == "__main__":
    unittest.main()