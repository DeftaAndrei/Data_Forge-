import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt
import pyarrow.parquet as pq 
from fuzzywuzzy import fuzz
from collections import defaultdict
import re
from datetime import datetime
import seaborn as sns
import os

def preprocess_text(text):
    if pd.isna(text):
        return ""
    text = str(text).lower()
    text = re.sub(r'[^\w\s]', '', text)
    return text.strip()

def identify_unique_and_duplicates(df, key_columns):
    """
    Identifică și separă companiile unice și duplicate bazate pe coloanele cheie.
    """
    # Creăm un ID compus din coloanele cheie
    df['composite_key'] = df[key_columns].apply(lambda x: '_'.join(x.astype(str)), axis=1)
    
    # Identificăm duplicatele
    duplicates_mask = df.duplicated(subset=['composite_key'], keep=False)
    
    # Separăm datele
    unique_companies = df[~duplicates_mask].copy()
    duplicate_companies = df[duplicates_mask].copy()
    
    return unique_companies, duplicate_companies

def analyze_and_separate_companies():
    print("Începe analiza și separarea companiilor unice și duplicate...")
    
    # Citim fișierul Excel
    print("\nCitire fișier Excel...")
    try:
        df = pd.read_excel('Tensorflow.xlsx')
        print(f"Număr total de rânduri: {len(df)}")
        print(f"Număr total de coloane: {len(df.columns)}")
    except Exception as e:
        print(f"Eroare la citirea fișierului Excel: {str(e)}")
        return
    
    # Definim coloanele pentru analiză
    columns_to_analyze = {
        'A': df.columns[0],
        'B': df.columns[1], 
        'C': df.columns[2],  
        'L': df.columns[11],
        'M': df.columns[12], 
        'AL': df.columns[37],
        'AM': df.columns[38],
        'AN': df.columns[39] 
    }
    
    # Identificăm companiile unice și duplicate
    key_columns = list(columns_to_analyze.values())
    unique_companies, duplicate_companies = identify_unique_and_duplicates(df, key_columns)
    
    print(f"\nRezultate identificare:")
    print(f"Număr total de companii: {len(df)}")
    print(f"Număr de companii unice: {len(unique_companies)}")
    print(f"Număr de companii duplicate: {len(duplicate_companies)}")
    
    # Analizăm duplicatele pentru fiecare coloană
    duplicate_analysis = {}
    for col_letter, col_name in columns_to_analyze.items():
        duplicates = df[df.duplicated(subset=[col_name], keep=False)]
        duplicate_analysis[col_letter] = {
            'column_name': col_name,
            'total_duplicates': len(duplicates),
            'unique_values': duplicates[col_name].nunique(),
            'most_common': duplicates[col_name].value_counts().head(5).to_dict()
        }
    
    # Salvăm rezultatele
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Creăm un director pentru rezultate
    result_dir = f'company_analysis_{timestamp}'
    os.makedirs(result_dir, exist_ok=True)
    
    # Salvăm companiile unice și duplicate în fișiere separate
    unique_companies.to_excel(f'{result_dir}/unique_companies.xlsx', index=False)
    duplicate_companies.to_excel(f'{result_dir}/duplicate_companies.xlsx', index=False)
    
    # Salvăm analiza detaliată
    with pd.ExcelWriter(f'{result_dir}/detailed_analysis.xlsx') as writer:
        # Statistici generale
        pd.DataFrame({
            'Metric': ['Total Companii', 'Companii Unice', 'Companii Duplicate'],
            'Valoare': [len(df), len(unique_companies), len(duplicate_companies)]
        }).to_excel(writer, sheet_name='Statistici Generale', index=False)
        
        # Analiza duplicatelor pe coloane
        duplicate_stats = []
        for col_letter, stats in duplicate_analysis.items():
            duplicate_stats.append({
                'Coloană': f"{col_letter} ({stats['column_name']})",
                'Total Duplicate': stats['total_duplicates'],
                'Valori Unice în Duplicate': stats['unique_values'],
                'Cele mai comune valori': str(list(stats['most_common'].items())[:3])
            })
        pd.DataFrame(duplicate_stats).to_excel(writer, sheet_name='Analiza Duplicatelor', index=False)
        
        # Exemple de duplicate pentru fiecare coloană
        for col_letter, col_name in columns_to_analyze.items():
            duplicates = df[df.duplicated(subset=[col_name], keep=False)].sort_values(col_name)
            if len(duplicates) > 0:
                duplicates.to_excel(writer, sheet_name=f'Duplicate_{col_letter}', index=False)
    
    # Creăm vizualizări
    # 1. Distribuția companiilor unice vs duplicate
    plt.figure(figsize=(10, 6))
    plt.pie([len(unique_companies), len(duplicate_companies)], 
            labels=['Companii Unice', 'Companii Duplicate'],
            autopct='%1.1f%%')
    plt.title('Distribuția Companiilor Unice vs Duplicate')
    plt.savefig(f'{result_dir}/distribution_pie.png')
    plt.close()
    
    # 2. Numărul de duplicate pe coloană
    plt.figure(figsize=(12, 6))
    duplicate_counts = [stats['total_duplicates'] for stats in duplicate_analysis.values()]
    plt.bar(columns_to_analyze.keys(), duplicate_counts)
    plt.title('Numărul de Duplicate pe Coloană')
    plt.xlabel('Coloană')
    plt.ylabel('Număr de Duplicate')
    plt.savefig(f'{result_dir}/duplicates_by_column.png')
    plt.close()
    
    print(f"\nRezultatele au fost salvate în directorul: {result_dir}")
    print("Fișiere generate:")
    print("1. unique_companies.xlsx - Companiile unice")
    print("2. duplicate_companies.xlsx - Companiile duplicate")
    print("3. detailed_analysis.xlsx - Analiza detaliată")
    print("4. distribution_pie.png - Vizualizare distribuție")
    print("5. duplicates_by_column.png - Vizualizare duplicate pe coloană")

if __name__ == "__main__":
    analyze_and_separate_companies()
