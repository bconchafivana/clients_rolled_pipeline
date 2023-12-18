import pandas as pd
import numpy as np
from db_conn import engine_ds , conn

client_dimension = pd.read_csv('/home/ec2-user/clients_rolled_pipeline/cliEjeLTC_contacts.csv')

customers = pd.read_sql("""
select SUBSTRING_INDEX(c.customer_id , '-', 1) as customer_id_l, SUBSTRING_INDEX(c.customer_id , '-', -1) as customer_id_r,
c.customer_id , e.name as admin_comercial_executive_name , e.zona , e.sucursal , c.city_name  , c.contact_phone as admin_contact_phone, 
c.contact_email as admin_contact_email , c.contact_name as admin_contact_name
from fc_customers c 
left join fc_executives e on e.executive_id = c.comercial_executive_id 
where sii_connected = 1 
 """ , conn)
customers['customer_id_l'] = customers['customer_id_l'].astype(str).astype(int)

cc = pd.merge(customers, client_dimension, left_on = 'customer_id_l', right_on = 'rut_cliente', how = 'outer'  )

cc.to_sql('clients_rolled', engine_ds, index=False, if_exists='replace')

table_name = 'datascience.clients_rolled'

# Replace 'read_user_bi' and 'read_user_risk' with the actual usernames
users_to_grant = ['read_user_bi', 'read_user_risk']

# Generate and execute the GRANT commands for each user
for user in users_to_grant:
    grant_command = f"GRANT SELECT ON TABLE {table_name} TO {user}"
    with engine_ds.connect() as connection:
        connection.execute(grant_command)
         
print("clients_rolled uploaded")