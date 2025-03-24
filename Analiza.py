import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt
import pyarrow.parquet as pq 
from fuzzywuzzy import fuzz
from collections import defaultdict
import re
from datetime import datetime
import seaborn as sns

def preprocess_text(text):
    if pd.isna(text):
        return ""
    text = str(text).lower()
    text = re.sub(r'[^\w\s]', '', text)
    return text.strip()

def analyze_excel_duplicates():
    print("Începe analiza și deduplicarea datelor din Excel...")
    
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
        'A': df.columns[0],  # Prima coloană
        'B': df.columns[1],  # A doua coloană
        'C': df.columns[2],  # A treia coloană
        'L': df.columns[11], # Coloana L
        'M': df.columns[12], # Coloana M
        'AL': df.columns[37], # Coloana AL
        'AM': df.columns[38], # Coloana AM
        'AN': df.columns[39]  # Coloana AN
    }
    
    print("\nColoanele analizate:")
    for col_letter, col_name in columns_to_analyze.items():
        print(f"Coloana {col_letter}: {col_name}")
    
    # Analiză pentru fiecare coloană
    print("\nAnaliză pentru fiecare coloană:")
    column_stats = {}
    
    for col_letter, col_name in columns_to_analyze.items():
        print(f"\nAnaliză pentru coloana {col_letter} ({col_name}):")
        print(f"Tip de date: {df[col_name].dtype}")
        print(f"Valori unice: {df[col_name].nunique()}")
        print(f"Valori lipsă: {df[col_name].isnull().sum()}")
        
        # Identificăm duplicatele
        duplicates = df[df.duplicated(subset=[col_name], keep=False)]
        print(f"Număr de rânduri duplicate: {len(duplicates)}")
        
        if len(duplicates) > 0:
            print("\nExemple de valori duplicate:")
            print(duplicates[col_name].value_counts().head())
        
        column_stats[col_letter] = {
            'nunique': df[col_name].nunique(),
            'missing': df[col_name].isnull().sum(),
            'duplicates': len(duplicates)
        }
    
    # Analiză similaritate între coloane
    print("\nAnaliză similaritate între coloane:")
    similarity_matrix = {}
    
    for col1_letter, col1_name in columns_to_analyze.items():
        for col2_letter, col2_name in columns_to_analyze.items():
            if col1_letter < col2_letter:  # Evităm compararea aceleiași coloane
                # Calculăm similaritatea folosind fuzzy matching
                similarity = 0
                count = 0
                for val1, val2 in zip(df[col1_name].dropna(), df[col2_name].dropna()):
                    similarity += fuzz.ratio(str(val1), str(val2))
                    count += 1
                if count > 0:
                    similarity = similarity / count
                    similarity_matrix[f"{col1_letter}-{col2_letter}"] = similarity
                    print(f"Similaritate între {col1_letter} și {col2_letter}: {similarity:.2f}%")
    
    # Salvăm rezultatele
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    print("\nSalvare rezultate...")
    with pd.ExcelWriter(f'duplicate_analysis_{timestamp}.xlsx') as writer:
        # Statistici generale
        stats_df = pd.DataFrame(column_stats).T
        stats_df.to_excel(writer, sheet_name='Statistici Generale')
        
        # Similaritate între coloane
        similarity_df = pd.DataFrame(similarity_matrix.items(), columns=['Coloane', 'Similaritate'])
        similarity_df.to_excel(writer, sheet_name='Similaritate Coloane', index=False)
        
        # Salvăm duplicatele pentru fiecare coloană
        for col_letter, col_name in columns_to_analyze.items():
            duplicates = df[df.duplicated(subset=[col_name], keep=False)]
            if len(duplicates) > 0:
                duplicates.to_excel(writer, sheet_name=f'Duplicate_{col_letter}', index=False)
    
    print(f"\nRezultatele au fost salvate în 'duplicate_analysis_{timestamp}.xlsx'")
    
    # Creăm vizualizări
    print("\nCreare vizualizări...")
    
    # 1. Grafic pentru numărul de valori unice și duplicate
    plt.figure(figsize=(12, 6))
    x = list(column_stats.keys())
    unique_values = [stats['nunique'] for stats in column_stats.values()]
    duplicate_values = [stats['duplicates'] for stats in column_stats.values()]
    
    plt.bar(x, unique_values, label='Valori Unice')
    plt.bar(x, duplicate_values, bottom=unique_values, label='Duplicate')
    plt.title('Distribuția valorilor unice și duplicate pe coloane')
    plt.xlabel('Coloane')
    plt.ylabel('Număr de rânduri')
    plt.legend()
    plt.savefig(f'duplicate_distribution_{timestamp}.png')
    plt.close()
    
    # 2. Heatmap pentru similaritate
    if similarity_matrix:
        similarity_data = np.zeros((len(columns_to_analyze), len(columns_to_analyze)))
        col_letters = list(columns_to_analyze.keys())
        
        for (col1, col2), similarity in similarity_matrix.items():
            i = col_letters.index(col1)
            j = col_letters.index(col2)
            similarity_data[i, j] = similarity
            similarity_data[j, i] = similarity
        
        plt.figure(figsize=(10, 8))
        sns.heatmap(similarity_data, 
                   xticklabels=col_letters,
                   yticklabels=col_letters,
                   annot=True,
                   fmt='.0f',
                   cmap='YlOrRd')
        plt.title('Similaritate între coloane (%)')
        plt.savefig(f'similarity_heatmap_{timestamp}.png')
        plt.close()
    
    print(f"\nVizualizările au fost salvate în:")
    print(f"1. duplicate_distribution_{timestamp}.png")
    print(f"2. similarity_heatmap_{timestamp}.png")

if __name__ == "__main__":
    analyze_excel_duplicates()
