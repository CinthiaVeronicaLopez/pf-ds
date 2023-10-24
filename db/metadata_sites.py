import gzip             # sobre el kernel 3.11 no se importa esta modulo
import ast              # modulo para interpretación del json en caracteres
import json             # modulo json para interpretacion
import pandas as pd
import re
import numpy as np
         
class MetadataSites:
    def __init__(self, db):
        self.db = db
        self.table_names = ["metadata_sites_hours", "metadata_misc_service_options", "metadata_misc_health_and_safety", "metadata_misc_accessibility", "metadata_misc_planning", "metadata_misc_payments", "metadata_relative_results", "metadata_categories", "metadata_sites"]    
        self.metadata_sites_hours = (
            "ID INT AUTO_INCREMENT NOT NULL",
            "gmap_id varchar(255) ", # 'gmap_id': '0x881614ce7c13acbb:0x5c7b18bbf6ec4f7e', )
            "hour_day varchar(255)", # 'Thursday'
            "hour_time varchar(255)", # '8AM–1:30PM
            "PRIMARY KEY (ID)"
        )
        self.metadata_sites_hours_fields = self.get_fields(self.metadata_sites_hours)
        #
        self.metadata_misc_service_options = (
            "ID INT AUTO_INCREMENT NOT NULL",
            "gmap_id varchar(255) ", # 'gmap_id': '0x881614ce7c13acbb:0x5c7b18bbf6ec4f7e', )
            "name varchar(255)", # 'Curbside pickup'
            "PRIMARY KEY (ID)"
        )
        self.metadata_misc_service_options_fields = self.get_fields(self.metadata_misc_service_options)
        #
        self.metadata_misc_health_and_safety = (
            "ID INT AUTO_INCREMENT NOT NULL",
            "gmap_id varchar(255) ", # 'gmap_id': '0x881614ce7c13acbb:0x5c7b18bbf6ec4f7e', )
            "name varchar(255)", # 'Mask required'
            "PRIMARY KEY (ID)"
        )
        self.metadata_misc_health_and_safety_fields = self.get_fields(self.metadata_misc_health_and_safety)
        #
        self.metadata_misc_accessibility = (
            "ID INT AUTO_INCREMENT NOT NULL",
            "gmap_id varchar(255) ", # 'gmap_id': '0x881614ce7c13acbb:0x5c7b18bbf6ec4f7e', )
            "name varchar(255)", # 'Wheelchair accessible entrance'
            "PRIMARY KEY (ID)"
        )
        self.metadata_misc_accessibility_fields = self.get_fields(self.metadata_misc_accessibility)
        #
        self.metadata_misc_planning = (
            "ID INT AUTO_INCREMENT NOT NULL",
            "gmap_id varchar(255) ", # 'gmap_id': '0x881614ce7c13acbb:0x5c7b18bbf6ec4f7e', )
            "name varchar(255)", # 'Quick visit'
            "PRIMARY KEY (ID)"
        )
        self.metadata_misc_planning_fields = self.get_fields(self.metadata_misc_planning)
        #
        self.metadata_misc_payments = (
            "ID INT AUTO_INCREMENT NOT NULL",
            "gmap_id varchar(255) ", # 'gmap_id': '0x881614ce7c13acbb:0x5c7b18bbf6ec4f7e', )
            "name varchar(255)", # 'Checks'
            "PRIMARY KEY (ID)"
        )
        self.metadata_misc_payments_fields = self.get_fields(self.metadata_misc_payments)
        #
        self.metadata_relative_results = (
            "ID INT AUTO_INCREMENT NOT NULL",
            "gmap_id varchar(255) ", # 'gmap_id': '0x881614ce7c13acbb:0x5c7b18bbf6ec4f7e', 
            "name varchar(255)", # '0x881614cd49e4fa33:0x2d507c24ff4f1c74'
            "PRIMARY KEY (ID)"
        )
        self.metadata_relative_results_fields = self.get_fields(self.metadata_relative_results)
        #
        self.metadata_categories = (
            "ID INT AUTO_INCREMENT NOT NULL",
            "gmap_id varchar(255) ", # 'gmap_id': '0x881614ce7c13acbb:0x5c7b18bbf6ec4f7e', 
            "name varchar(255)", # 'Pharmacy'
            "PRIMARY KEY (ID)"
        )
        self.metadata_categories_fields = self.get_fields(self.metadata_categories)
        #
        self.metadata_sites = (
            "ID INT AUTO_INCREMENT NOT NULL",
            "name varchar(255)", # 'name': 'Walgreens Pharmacy', 
            "address varchar(255)", # 'address': 'Walgreens Pharmacy, 124 E North St, Kendallville, IN 46755',
            "gmap_id varchar(255)", # 'gmap_id': '0x881614ce7c13acbb:0x5c7b18bbf6ec4f7e', 
            "description varchar(255)", # 'description': 'Department of the Walgreens chain providing prescription medications & other health-related items.', 
            "latitude float", # 'latitude': 41.451859999999996, 
            "longitude float", # 'longitude': -85.2666757, 
            "category varchar(255)", # 'category': ['Pharmacy'], 
            "avg_rating float", # 'avg_rating': 4.2, 
            "num_of_reviews integer", # 'num_of_reviews': 5,
            "price varchar(255)", # 'price': '$$', 
            "hours varchar(255)", # 'hours': [['Thursday', '8AM–1:30PM'], ['Friday', '8AM–1:30PM'], ['Saturday', '9AM–1:30PM'], ['Sunday', '10AM–1:30PM'], ['Monday', '8AM–1:30PM'], ['Tuesday', '8AM–1:30PM'], ['Wednesday', '8AM–1:30PM']], 
            # MISC {
            "misc_service_options varchar(255)", # 'Service options': ['Curbside pickup', 'Drive-through', 'In-store pickup', 'In-store shopping'], 
            "misc_health_and_safety varchar(255)", # 'Health & safety': ['Mask required', 'Staff wear masks', 'Staff get temperature checks'], 
            "misc_accessibility varchar(255)", # 'Accessibility': ['Wheelchair accessible entrance', 'Wheelchair accessible parking lot'], 
            "misc_planning varchar(255)", # 'Planning': ['Quick visit'],  
            "misc_payments varchar(255)", # 'Payments': ['Checks', 'Debit cards']
            # }
            "state varchar(255)", # 'state': 'Closes soon ⋅ 1:30PM ⋅ Reopens 2PM', 
            "relative_results varchar(255)", # 'relative_results': ['0x881614cd49e4fa33:0x2d507c24ff4f1c74', '0x8816145bf5141c89:0x535c1d605109f94b', '0x881614cda24cc591:0xca426e3a9b826432',    '0x88162894d98b91ef:0xd139b34de70d3e03', '0x881615400b5e57f9:0xc56d17dbe420a67f'], 
            "url varchar(255)", # 'url': 'https://www.google.com/maps/place//data=!4m2!3m1!1s0x881614ce7c13acbb:0x5c7b18bbf6ec4f7e?authuser=-1&hl=en&gl=us'
            "PRIMARY KEY (ID)"
            # ,"FOREIGN KEY (hours) REFERENCES metadata_sites_hours(ID)"
        )
        self.metadata_sites_fields = self.get_fields(self.metadata_sites)
        self.create()
    #
    def get_fields(self, fields):
        """
        Obtiene los campos quitando los tipos de datos.

        Args:
        fields (list): Una lista de campos.

        Returns:
        fields (list): Una lista de campos.
        """
        # remove ID
        fields = fields[1:]
        # remove "PRIMARY KEY (ID)"
        fields = fields[:-1]
        fields = [field.split()[0] for field in fields]

        print("fields", fields)
        return fields
    #
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
        """
        Insertaen la tabla 'metadata_sites' de la base de datos.

        Args:
        data_dic (dict): Un diccionario.

        Returns:
        None
        """
        # data = {'name': 'Porter Pharmacy', 'address': 'Porter Pharmacy, 129 N Second St, Cochran, GA 31014', 'gmap_id': '0x88f16e41928ff687:0x883dad4fd048e8f8', 'description': None, 'latitude': 32.3883, 'longitude': -83.3571, 'category': ['Pharmacy'], 'avg_rating': 4.9, 'num_of_reviews': 16, 'price': None, 'hours': [['Friday', '8AM–6PM'], ['Saturday', '8AM–12PM'], ['Sunday', 'Closed'], ['Monday', '8AM–6PM'], ['Tuesday', '8AM–6PM'], ['Wednesday', '8AM–12PM'], ['Thursday', '8AM–6PM']], 'MISC': {'Service options': ['In-store shopping', 'Same-day delivery'], 'Health & safety': ['Mask required', 'Staff required to disinfect surfaces between visits'], 'Accessibility': ['Wheelchair accessible entrance'], 'Planning': ['Quick visit']}, 'state': 'Open ⋅ Closes 6PM', 'relative_results': ['0x88f16e41929435cf:0x5b2532a2885e9ef6', '0x88f16c32716531c1:0x5f19bdaa5044e4fa', '0x88f16e6e3f4a21df:0xcf495da9bb4d89ea'], 'url': 'https://www.google.com/maps/place//data=!4m2!3m1!1s0x88f16e41928ff687:0x883dad4fd048e8f8?authuser=-1&hl=en&gl=us'}
        id = data['gmap_id']
        data_list = []
        # tabla relacionada hours
        hours = data['hours']
        for hour in hours:
            hours_list = []
            hours_list.append(id)
            hours_list.append(hour[0])
            hours_list.append(hour[1])
            hours_values = tuple(hours_list)
            self.db.insert_data("metadata_sites_hours", self.metadata_sites_hours_fields, hours_values)
        data['hours'] = id
        #
        # tabla relacionada misc_service_options
        misc_service_options = data['MISC']['Service options']
        for misc_service_option in misc_service_options:
            misc_service_options_list = []
            misc_service_options_list.append(id)
            misc_service_options_list.append(misc_service_option)
            misc_service_options_values = tuple(misc_service_options_list)
            self.db.insert_data("metadata_misc_service_options", self.metadata_misc_service_options_fields, misc_service_options_values)
        #
        # tabla relacionada misc_health_and_safety
        misc_health_and_safety = data['MISC']['Health & safety']
        for misc_health_and_safety in misc_health_and_safety:
            misc_health_and_safety_list = []
            misc_health_and_safety_list.append(id)
            misc_health_and_safety_list.append(misc_health_and_safety)
            misc_health_and_safety_values = tuple(misc_health_and_safety_list)
            self.db.insert_data("metadata_misc_health_and_safety", self.metadata_misc_health_and_safety_fields, misc_health_and_safety_values)
        #
        # tabla relacionada misc_accessibility
        misc_accessibility = data['MISC']['Accessibility']
        for misc_accessibility in misc_accessibility:
            misc_accessibility_list = []
            misc_accessibility_list.append(id)
            misc_accessibility_list.append(misc_accessibility)
            misc_accessibility_values = tuple(misc_accessibility_list)
            self.db.insert_data("metadata_misc_accessibility", self.metadata_misc_accessibility_fields, misc_accessibility_values)
        #
        # tabla relacionada misc_planning
        
        misc_planning = data['MISC']['Planning']
        for misc_planning in misc_planning:
            misc_planning_list = []
            misc_planning_list.append(id)
            misc_planning_list.append(misc_planning)
            misc_planning_values = tuple(misc_planning_list)
            self.db.insert_data("metadata_misc_planning", self.metadata_misc_planning_fields, misc_planning_values)
        #
        # tabla relacionada misc_payments
        if 'Payments' in data['MISC']:
            misc_payments = data['MISC']['Payments']
            for misc_payment in misc_payments:
                misc_payments_list = []
                misc_payments_list.append(id)
                misc_payments_list.append(misc_payment)
                misc_payments_values = tuple(misc_payments_list)
                self.db.insert_data("metadata_misc_payments", self.metadata_misc_payments_fields, misc_payments_values)
         
        #
        # tabla relacionada relative_results
        relative_results = data['relative_results']
        for relative_result in relative_results:
            relative_results_list = []
            relative_results_list.append(id)
            relative_results_list.append(relative_result)
            relative_results_values = tuple(relative_results_list)
            self.db.insert_data("metadata_relative_results", self.metadata_relative_results_fields, relative_results_values)
            
        #

        data['misc_service_options'] = id
        data['misc_health_and_safety'] = id
        data['misc_accessibility'] = id
        data['misc_planning'] = id
        data['misc_payments'] = id
        data['relative_results'] = id
        # tabla relacionada category
        categories = data['category']        
        for category in categories:
            categories_list = []
            categories_list.append(id)
            categories_list.append(category)
            categories_values = tuple(categories_list)
            self.db.insert_data("metadata_categories", self.metadata_categories_fields, categories_values)
        data['category'] = id
        #
        data.pop('MISC', None)
        # data.pop('gmap_id', None)

        # print("data", data)
        for item in data:
            if data[item] == None:
                data[item] = "null"
            elif type(data[item]) == list:
                data[item] = str(data[item])
            elif type(data[item]) == dict:
                data[item] = str(data[item])
            else:
                data[item] = str(data[item])
            data_list.append(data[item])

        data_values = tuple(data_list)
        self.db.insert_data("metadata_sites", self.metadata_sites_fields, data_values)
        # print("Termino la importación de metadata_sites!!!")
   