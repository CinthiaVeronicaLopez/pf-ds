from db import Database

db = Database()
# create review_states
# db.drop_table("review_states")
review_states_fields = (
    "user_id integer", # '101463350189962023774
    "name varchar", # 'Jordan Adams'
    "time integer", # 1627750414677
    "rating integer", # 5
    "text varchar", # 'Cool place, great people, awesome dentist!'  
    "pics varchar", # [{'url': ['https://lh5.googleusercontent.com/p/AF1QipNq2nZC5TH4_M7h5xRAdZ61hoTgvY1o9lozABguI=w150-h150-k-no-p']}
    "resp varchar", #{
       #'time': 1628455067818, 
       #'text': 'Thank you for your five-star review! -Dr. Blake'
    "gmap_id varchar"), # '0x87ec2394c2cd9d2d:0xd1119cfbee0da6f3

db.create_table("review_states", review_states_fields)

import_dic('./assets/australian_review_states.json', insert_review_states)