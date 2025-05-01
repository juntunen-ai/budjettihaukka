from langgraph.graph import StateGraph
from typing import Dict, Any

# Määritellään yksinkertainen graafi
def simple_test():
    try:
        # Määritellään tilagraafi
        state_graph = StateGraph(Dict[str, Any])

        # Lisätään solmuja
        state_graph.add_node("start", lambda state: {"message": "Hello from LangGraph!"})

        # Määritellään siirtymät
        state_graph.set_entry_point("start")

        # Koosta graafi
        graph = state_graph.compile()

        # Aja graafi
        result = graph.invoke({"input": "test"})

        print("LangGraph test successful!")
        print(f"Result: {result}")
        return True
    except Exception as e:
        print(f"LangGraph test failed: {e}")
        return False

if __name__ == "__main__":
    simple_test()