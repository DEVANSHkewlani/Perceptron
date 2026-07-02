import asyncio
import sys
import os

# Add the workspace root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from reasoning.engine import ReasoningEngine

async def main():
    engine = ReasoningEngine()
    print("Fetching situation...")
    situation = await engine._fetch_situation()
    print(f"Situation fetched: {situation is not None}")
    if situation:
        print(f"Ranked anomalies: {situation.get('ranked_anomalies')}")
    
    print("Running reason()...")
    try:
        decision = await engine.reason()
        print(f"Decision result: {decision}")
    except Exception as e:
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
