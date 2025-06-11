"""
run_pipeline.py
---------------
Script CLI : python run_pipeline.py

• Exécute rating_batch.ipynb  → génère rated_cdrs/
• Exécute billing_batch.ipynb → génère invoices_final/
"""

import sys, time, papermill as pm

def run_notebook(path_in: str, path_out: str):
    print(f"▶ Running {path_in} …")
    tic = time.time()
    pm.execute_notebook(
        input_path  = path_in,
        output_path = path_out,
        parameters  = {}          
    )
    print(f"✓ Finished {path_in} in {time.time()-tic:.1f}s\n")

def main():
    try:
        # 1) Rating Engine
        run_notebook("RatingEngine.ipynb",
                     "RatingEngine.ipynb")

        # 2) Billing Engine
        run_notebook("BillingEngine.ipynb",
                     "BillingEngine.ipynb")

        # 2) Billing Engine
        run_notebook("reporting.ipynb",
                     "reporting.ipynb")
        print("🎉 Pipeline terminé avec succès.")
    except Exception as e:
        print("❌ Pipeline échoué :", e)
        sys.exit(1)

if __name__ == "__main__":
    main()
