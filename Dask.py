import pandas as pd 
import numpy as np
import dask.dataframe as dd
import matplotlib.pyplot as plt
import seaborn as sns
from dask.diagnostics import ProgressBar
import os

# Configurăm opțiunile de afișare pentru dask
dd.set_options(display_max_rows=75)

def debug_file_existence():
    """Verifică dacă fișierul parquet există și afișează informații despre el."""
    file_path = 'Tensorflow.parquet'
    if os.path.exists(file_path):
        print(f"\nFișierul {file_path} există!")
        print(f"Dimensiune: {os.path.getsize(file_path) / (1024*1024):.2f} MB")
    else:
        print(f"\nERROR: Fișierul {file_path} nu există în directorul curent!")
        print(f"Director curent: {os.getcwd()}")
        print("Fișiere disponibile în directorul curent:")
        print("\n".join(os.listdir(".")))
        raise FileNotFoundError(f"Fișierul {file_path} nu a fost găsit!")

def analyze_large_dataset():
    try:
        print("Începe analiza setului mare de date folosind Dask...")
        
        # Verificăm existența fișierului
        debug_file_existence()
        
        # Setăm opțiuni pentru pandas (nu pentru dask)
        pd.set_option('display.max_rows', 75)
        
        print("\nCitire fișier parquet...")
        try:
            df = dd.read_parquet('Tensorflow.parquet')
            print("Fișierul parquet a fost citit cu succes!")
        except Exception as e:
            print(f"Eroare la citirea fișierului parquet: {str(e)}")
            raise
        
        # Afișăm informații despre schema datelor
        print("\nSchema setului de date:")
        try:
            print(df.dtypes)
            print(f"\nColoane disponibile: {list(df.columns)}")
        except Exception as e:
            print(f"Eroare la afișarea schemei: {str(e)}")
            raise
        
        # Calculăm statistici de bază
        print("\nCalcul statistici de bază...")
        try:
            with ProgressBar():
                # Verificăm dacă dataframe-ul nu este gol
                if df.npartitions > 0:
                    # Număr total de rânduri
                    total_rows = len(df.compute())
                    print(f"\nNumăr total de rânduri: {total_rows:,}")
                    print(f"Număr de partiții: {df.npartitions}")
                    
                    # Statistici pentru coloanele numerice
                    print("\nStatistici pentru coloanele numerice:")
                    numeric_stats = df.describe().compute()
                    print(numeric_stats)
                    
                    # Verificăm valorile lipsă
                    print("\nValori lipsă în fiecare coloană:")
                    missing_values = df.isnull().sum().compute()
                    print(missing_values[missing_values > 0])
                else:
                    print("AVERTISMENT: DataFrame-ul este gol!")
        except Exception as e:
            print(f"Eroare la calculul statisticilor: {str(e)}")
            raise
        
        # Salvăm statisticile într-un fișier Excel
        print("\nSalvare statistici în Excel...")
        try:
            with pd.ExcelWriter('dask_analysis_results.xlsx') as writer:
                numeric_stats.to_excel(writer, sheet_name='Statistici Numerice')
                missing_values.to_frame('Valori Lipsă').to_excel(writer, sheet_name='Valori Lipsă')
        except Exception as e:
            print(f"Eroare la salvarea în Excel: {str(e)}")
            print("Continuăm cu restul analizei...")
        
        # Creăm vizualizări pentru distribuția datelor
        print("\nCreare vizualizări...")
        try:
            # Selectăm primele 100,000 de rânduri pentru vizualizare
            sample_df = df.head(n=33447, compute=True)
            
            # Creăm mai multe subploturi pentru coloanele numerice
            numeric_cols = sample_df.select_dtypes(include=[np.number]).columns
            n_cols = len(numeric_cols)
            
            if n_cols > 0:
                fig, axes = plt.subplots(n_cols, 1, figsize=(12, 5*n_cols))
                if n_cols == 1:
                    axes = [axes]
                
                for i, col in enumerate(numeric_cols):
                    sns.histplot(data=sample_df, x=col, ax=axes[i])
                    axes[i].set_title(f'Distribuția pentru {col}')
                
                plt.tight_layout()
                plt.savefig('distributions.png')
                plt.close()
        except Exception as e:
            print(f"Eroare la crearea vizualizărilor: {str(e)}")
            print("Continuăm cu finalizarea analizei...")
        
        print("\nAnaliză completă!")
        print("Rezultatele au fost salvate în:")
        print("1. dask_analysis_results.xlsx")
        print("2. distributions.png (pentru vizualizări)")
        
        return df
        
    except Exception as e:
        print(f"\nEroare în timpul analizei: {str(e)}")
        print("Stacktrace complet:")
        import traceback
        print(traceback.format_exc())
        raise

if __name__ == "__main__":
    # Măsurăm timpul de execuție
    import time
    start_time = time.time()
    
    try:
        df = analyze_large_dataset()
        end_time = time.time()
        print(f"\nTimp total de execuție: {end_time - start_time:.2f} secunde")
    except Exception as e:
        print("\nProgramul s-a încheiat cu erori.")
        end_time = time.time()
        print(f"Timp de execuție până la eroare: {end_time - start_time:.2f} secunde")


