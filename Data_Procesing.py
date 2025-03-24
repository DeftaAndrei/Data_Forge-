import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import matplotlib.pyplot as plt
import seaborn as sns
import dask.dataframe as dd
from fuzzywuzzy import fuzz
from collections import defaultdict
import re
from datetime import datetime

def preprocess_text(text):
    if pd.isna(text):
        return ""
    text = str(text).lower()
    text = re.sub(r'[^\w\s]', '', text)
    return text.strip()

def analyze_company_data():
    print("Începe analiza detaliată a datelor companiilor...")
    
    # Citim datele
    print("\nCitire fișiere parquet...")
    table1 = pq.read_table('Tensorflow.parquet')
    table2 = pq.read_table('veridion_entity_resolution_challenge.snappy.parquet')
    
    data_set1 = table1.to_pandas()
    data_set2 = table2.to_pandas()
    
    # Lista de câmpuri pentru analiză
    fields_to_analyze = [
        'website_url', 'primary_email', 'primary_phone', 'domains',
        'all_domains', 'company_legal_names', 'company_name',
        'main_country', 'main_city', 'main_street', 'main_street_number',
        'main_latitude', 'main_longitude', 'year_founded',
        'business_tags', 'main_business_category', 'main_industry',
        'main_sector', 'sic_codes', 'naics_2022_primary_code'
    ]
    
    # Verificăm existența câmpurilor în seturile de date
    print("\nVerificare câmpuri disponibile în seturile de date:")
    print("\nSet 1 (Tensorflow):")
    available_fields1 = [field for field in fields_to_analyze if field in data_set1.columns]
    print(f"Câmpuri disponibile: {len(available_fields1)}")
    print(available_fields1)
    
    print("\nSet 2 (Veridion):")
    available_fields2 = [field for field in fields_to_analyze if field in data_set2.columns]
    print(f"Câmpuri disponibile: {len(available_fields2)}")
    print(available_fields2)
    
    # Analiză statistică pentru fiecare câmp
    print("\nAnaliză statistică pentru fiecare câmp:")
    stats = {}
    
    for field in available_fields1:
        if field in data_set1.columns:
            print(f"\nAnaliză pentru {field}:")
            print(f"Tip de date: {data_set1[field].dtype}")
            print(f"Valori unice: {data_set1[field].nunique()}")
            print(f"Valori lipsă: {data_set1[field].isnull().sum()}")
            
            if data_set1[field].dtype in ['int64', 'float64']:
                print(f"Statistici numerice:")
                print(data_set1[field].describe())
            else:
                print("Primele 5 valori unice:")
                print(data_set1[field].unique()[:5])
    
    # Identificăm potențiale duplicate bazate pe diferite criterii
    print("\nIdentificare potențiale duplicate:")
    
    # 1. După website_url
    if 'website_url' in data_set1.columns:
        website_duplicates = data_set1[data_set1.duplicated(subset=['website_url'], keep=False)]
        print(f"\nNumăr de companii cu website-uri duplicate: {len(website_duplicates)}")
    
    # 2. După email
    if 'primary_email' in data_set1.columns:
        email_duplicates = data_set1[data_set1.duplicated(subset=['primary_email'], keep=False)]
        print(f"Număr de companii cu email-uri duplicate: {len(email_duplicates)}")
    
    # 3. După nume companie (folosind fuzzy matching)
    if 'company_name' in data_set1.columns:
        print("\nAnaliză similaritate nume companii...")
        company_names = data_set1['company_name'].dropna().unique()
        similar_companies = defaultdict(list)
        
        for i, name1 in enumerate(company_names[:100]):  # Limităm la primele 100 pentru performanță
            for name2 in company_names[i+1:]:
                similarity = fuzz.ratio(str(name1), str(name2))
                if similarity > 85:  # Prag de similaritate
                    similar_companies[name1].append((name2, similarity))
        
        print(f"Număr de companii cu nume similare: {len(similar_companies)}")
        if similar_companies:
            print("\nExemple de companii cu nume similare:")
            for name, similar in list(similar_companies.items())[:5]:
                print(f"\n{name}:")
                for similar_name, similarity in similar:
                    print(f"- {similar_name} (similaritate: {similarity}%)")
    
    # Salvăm rezultatele
    print("\nSalvare rezultate...")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Salvăm statistici în Excel
    with pd.ExcelWriter(f'company_analysis_{timestamp}.xlsx') as writer:
        # Statistici generale
        stats_df = pd.DataFrame({
            'Câmp': available_fields1,
            'Tip Date': [data_set1[field].dtype for field in available_fields1],
            'Valori Unice': [data_set1[field].nunique() for field in available_fields1],
            'Valori Lipsă': [data_set1[field].isnull().sum() for field in available_fields1]
        })
        stats_df.to_excel(writer, sheet_name='Statistici Generale', index=False)
        
        # Exemple de duplicate
        if 'website_url' in data_set1.columns:
            website_duplicates.to_excel(writer, sheet_name='Website Duplicate', index=False)
        if 'primary_email' in data_set1.columns:
            email_duplicates.to_excel(writer, sheet_name='Email Duplicate', index=False)
    
    print(f"\nRezultatele au fost salvate în 'company_analysis_{timestamp}.xlsx'")
    
    return data_set1, data_set2

if __name__ == "__main__":
    data_set1, data_set2 = analyze_company_data()










