import pandas as pd
import time
from backend import app_graph, db
from langchain_community.utilities import SQLDatabase

# --- 1. THE GOLDEN DATASET (Ground Truth) ---
# You must define what the "Right Answer" (Gold SQL) is for these questions.
test_dataset = [
    {
        "question": "Show me the top 3 products by price.",
        "gold_sql": "SELECT product_name, price FROM products ORDER BY price DESC LIMIT 3"
    },
    {
        "question": "Count the total number of sales.",
        "gold_sql": "SELECT COUNT(*) FROM sales"
    },
    {
        "question": "What is the price of Laptop Pro?",
        "gold_sql": "SELECT price FROM products WHERE product_name = 'Laptop Pro'"
    },
    {
        "question": "List email addresses of all customers.",
        "gold_sql": "SELECT email FROM customers"
    },
    {
        "question": "Show me the sales happened in january regionwise.",
        "gold_sql": "SELECT r.region_name, SUM(s.total_amount) AS total_sales FROM sales s JOIN regions r ON s.region_id = r.region_id WHERE STRFTIME('%m', s.sale_date) = '01' GROUP BY r.region_name"
    },
    {
        "question": "List all the products with their sales amount in 2023",
        "gold_sql": "SELECT p.product_name, SUM(s.total_amount) AS total_sales FROM products p JOIN sales s ON p.product_id = s.product_id WHERE STRFTIME('%Y', s.sale_date) = '2023' GROUP BY p.product_name"
    },
    {
        "question": "List all the customers in descending order of their joining date.",
        "gold_sql": "SELECT * FROM customers ORDER BY join_date DESC;"
    },
    {
        "question": "List total sales amount in 2024.",
        "gold_sql": "SELECT SUM(total_amount) FROM sales WHERE sale_date LIKE '2024%';"
    },
    {
        "question": "List all the customers who like to dance.",
        "gold_sql": "SELECT * FROM customers WHERE customer_name LIKE '%dance%' OR email LIKE '%dance%';"
    },
    {
        "question": "List all the regions where the sales didn't happen at all.",
        "gold_sql": "SELECT region_name FROM regions WHERE region_id NOT IN (SELECT region_id FROM sales)"
    },
]

def run_evaluation():
    print("🚀 Starting Evaluation...\n")
   
    results = []
    total_latency = 0
    correct_count = 0
    valid_sql_count = 0

    for i, case in enumerate(test_dataset):
        q = case['question']
        gold_sql = case['gold_sql']
       
        print(f"Test {i+1}: {q}")
       
        # 1. Measure Latency
        start_time = time.time()
       
        # 2. Invoke Agent
        try:
            inputs = {"question": q, "retry_count": 0}
            response = app_graph.invoke(inputs)
            gen_sql = response.get("sql_query", "")
            gen_error = response.get("error", None)
           
            latency = time.time() - start_time
            total_latency += latency
           
            # 3. Check Validity (Did it crash?)
            if not gen_error and gen_sql != "IMPOSSIBLE":
                valid_sql_count += 1
               
                # 4. EXECUTION ACCURACY CHECK
                # Run Gold SQL
                df_gold = pd.read_sql_query(gold_sql, db._engine)
                # Run Generated SQL
                try:
                    df_gen = pd.read_sql_query(gen_sql, db._engine)
                   
                    # Compare Dataframes (Ignore order of columns/rows for flexible matching)
                    # We normalize by sorting values to ensure fairness
                    matches = df_gold.equals(df_gen)
                   
                    # If direct match fails, try relaxed match (sets of values)
                    if not matches and not df_gold.empty and not df_gen.empty:
                         # Convert to sets of tuples for comparison
                         set_gold = set([tuple(x) for x in df_gold.values])
                         set_gen = set([tuple(x) for x in df_gen.values])
                         matches = (set_gold == set_gen)

                except Exception as e:
                    matches = False
                    print(f"  ❌ SQL Execution Failed: {e}")
            else:
                matches = False
                print(f"  ❌ Agent Failed/Error: {gen_error}")

            # Record Result
            if matches:
                print("  ✅ PASS")
                correct_count += 1
            else:
                print("  ❌ FAIL")
                print(f"     Expected: {gold_sql}")
                print(f"     Got:      {gen_sql}")

            results.append({
                "question": q,
                "passed": matches,
                "latency": latency
            })
           
        except Exception as e:
            print(f"  ❌ System Crash: {e}")

    # --- FINAL REPORT ---
    total = len(test_dataset)
    accuracy = (correct_count / total) * 100
    validity = (valid_sql_count / total) * 100
    avg_lat = total_latency / total

    print("\n" + "="*30)
    print("📊 EVALUATION REPORT")
    print("="*30)
    print(f"Total Tests:      {total}")
    print(f"Execution Accuracy: {accuracy:.2f}%  <-- Your Main Metric")
    print(f"Valid SQL Rate:     {validity:.2f}%")
    print(f"Avg Latency:        {avg_lat:.2f}s")
    print("="*30)

if __name__ == "__main__":
    run_evaluation()