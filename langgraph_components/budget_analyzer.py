from langgraph.graph import StateGraph
from typing import Dict, Any

def create_budget_analyzer():
    state_graph = StateGraph(Dict[str, Any])
    state_graph.add_node("analyze_budget", lambda state: {"analysis": "Budget analysis complete"})
    state_graph.set_entry_point("analyze_budget")
    return state_graph.compile()