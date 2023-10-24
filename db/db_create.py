import gzip             
import ast              
import json             
import pandas as pd
import re
import numpy as np
from db import Database

from metadata_sites import MetadataSites

db = Database()


def import_dic(file_path, insert):
    # abrimos el archivo json con encoding utf-8 y lo guardamos como 'f'
    with open(file_path, 'r', encoding='utf-8') as f:
        print(f"Importando {file_path}...")
        for line in f.readlines():
            # print("line", line)
            # convertimos el string a diccionario
            data_dic = ast.literal_eval(line)
            # print("data_dic", data_dic)
            insert(data_dic)
        print(f"Termino la importación de {file_path}!!!")


def import_json(file_path, insert):
    # Lee el archivo JSON
    with open(file_path, 'r') as f:
        print(f"Importando {file_path}...")
        for line in f.readlines():
            # print("line", line)
            data = json.loads(line)
            # convierte el JSON en un diccionario
            data_dic = dict(data)
            # print("data_dic", data_dic)
            insert(data_dic)
            break
        print(f"Termino la importación de {file_path}!!!")


# import metadata_sites
metadata_sites = MetadataSites(db)
# assets\metadata_sites\1.json
import_json('./assets/metadata_sites/1.json', metadata_sites.insert)

# Select all data from the table
# results = db.select_data("metadata_sites")

# Print the results
results= "termino!!!"
print(results)

# Close the connection
db.close()