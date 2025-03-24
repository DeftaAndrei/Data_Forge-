import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt
import pyarrow.parquet as pq 

# Citim fișierele parquet folosind pyarrow
table1 = pq.read_table('Tensorflow.parquet')
table2 = pq.read_table('veridion_entity_resolution_challenge.snappy.parquet')

# Convertim în DataFrame pentru vizualizare mai ușoară
data_set1 = table1.to_pandas()
data_set2 = table2.to_pandas()

# Afișăm informații despre structura datelor
print("Informații despre primul set de date (Tensorflow):")
print(f"Număr de rânduri: {len(data_set1)}")
print(f"Coloane: {data_set1.columns.tolist()}")
print("\nPrimele câteva rânduri:")
print(data_set1.head())

print("\nInformații despre al doilea set de date (Veridion):")
print(f"Număr de rânduri: {len(data_set2)}")
print(f"Coloane: {data_set2.columns.tolist()}")
print("\nPrimele câteva rânduri:")
print(data_set2.head())