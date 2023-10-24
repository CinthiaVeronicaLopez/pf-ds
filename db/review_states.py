import gzip             # sobre el kernel 3.11 no se importa esta modulo
import ast              # modulo para interpretación del json en caracteres
import json             # modulo json para interpretacion
import pandas as pd
import re
import numpy as np

from db import Database

db = Database()
# create review_states
# db.drop_table("review_states")
class ReviewStates:
    def __init__(self, db):
        self.db = db
        self.table_name = ["review_states_pics_url"]
        self.review_states_pics_url = (
            "ID INT AUTO_INCREMENT NOT NULL",
            "gmap_id varchar(255) ", 
            "name varchar(255)", 
            "PRIMARY KEY (ID)"
        )
        self.review_states_pics_url_fields = self.get_fields(self.review_states_pics_url)

self.review_states_fields = (
    "ID INT AUTO_INCREMENT NOT NULL",
    "user_id integer", # '101463350189962023774
    "name varchar (255)", # 'Jordan Adams'
    "time integer", # 1627750414677
    "rating integer (255)", # 5
    "text varchar (255)", # 'Cool place, great people, awesome dentist!'  
    "pics_url varchar(255)", # [{'url': ['https://lh5.googleusercontent.com/p/AF1QipNq2nZC5TH4_M7h5xRAdZ61hoTgvY1o9lozABguI=w150-h150-k-no-p']}
    "resp_time varchar(255)", #{
       #'time': 1628455067818, 
    "resp_text varchar(255)", 
       #'text': 'Thank you for your five-star review! -Dr. Blake'
    "gmap_id varchar(255)" # '0x87ec2394c2cd9d2d:0xd1119cfbee0da6f3
)
self.review_states_fields = self.get_fields(self.review_states_fields)
self.create()
#
def get_fields(self, fields):
    # remove ID
        fields = fields[1:]
        # remove "PRIMARY KEY (ID)"
        fields = fields[:-1]
        fields = [field.split()[0] for field in fields]
        print("fields", fields)
        return fields
#
def create(self):
     def create(self):
        """
        Crea las tablas de la base de datos.

        Args:
        None

        Returns:
        None
        """        
        for table_name in self.table_names:
            # self.db.truncate_table(table_name)
            self.db.drop_table(table_name)
            self.db.create_table(table_name, getattr(self, table_name))

#
def insert(self, data):

#db.create_table("review_states", review_states_fields)
