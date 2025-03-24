import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt
import pyarrow.parquet as pq 
from fuzzywuzzy import fuzz
from collections import defaultdict
import re

def preprocess_text(text):
    if pd.isna(text):
        return ""
    # Convertim în lowercase și eliminăm caracterele speciale
    text = str(text).lower()
    text = re.sub(r'[^\w\s]', '', text)
    return text.strip()

def calculate_similarity(str1, str2):
    if pd.isna(str1) or pd.isna(str2):
        return 0
    return fuzz.ratio(str1, str2)

def analyze_companies():
    print("Începe analiza companiilor...")
    
    # Citim datele
    table = pq.read_table('Tensorflow.parquet')
    df = table.to_pandas()
    
    print(f"\nNumăr total de înregistrări: {len(df)}")
    
    # Afișăm informații despre coloanele disponibile
    print("\nColoanele disponibile în setul de date:")
    print(df.columns.tolist())
    
    print("\nPrimele câteva rânduri din set:")
    print(df.head())
    
    # Verificăm tipurile de date pentru fiecare coloană
    print("\nTipurile de date pentru fiecare coloană:")
    print(df.dtypes)
    
    # Verificăm valorile lipsă
    print("\nValori lipsă în fiecare coloană:")
    print(df.isnull().sum())
    
    # Salvăm informațiile despre structura datelor într-un fișier text
    with open('data_structure_info.txt', 'w', encoding='utf-8') as f:
        f.write("Structura setului de date:\n\n")
        f.write(f"Număr total de înregistrări: {len(df)}\n\n")
        f.write("Coloane disponibile:\n")
        for col in df.columns:
            f.write(f"- {col} ({df[col].dtype})\n")
        f.write("\nExemplu de date (primele 5 rânduri):\n")
        f.write(df.head().to_string())
    
    print("\nInformațiile despre structura datelor au fost salvate în 'data_structure_info.txt'")
    
    return df

if __name__ == "__main__":
    df = analyze_companies()
    print("\nAnaliză completă. Verificați fișierul 'data_structure_info.txt' pentru detalii complete.")


