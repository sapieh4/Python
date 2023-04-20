import webbrowser
import pandas as pd
from sqlalchemy import create_engine, text
import os
import os.path
import time

webbrowser.register('chrome',
	None,
	webbrowser.BackgroundBrowser("C:\Program Files\Google\Chrome\Application\chrome.exe"))

# connect database and get data--------------------

##########################
#     ERP PROD DB        #
##########################

POSTGRES_ADDRESS = '192.168.203.205'
POSTGRES_PORT = '5432'
POSTGRES_USERNAME = 'Service'  ## CHANGE THIS TO YOUR PANOPLY/POSTGRES USERNAME
POSTGRES_PASSWORD = 'pMk1U3VxQi4aA66ufGaC'  ## CHANGE THIS TO YOUR PANOPLY/POSTGRES PASSWORD
POSTGRES_DBNAME = 'prod'  ## CHANGE THIS TO YOUR DATABASE NAME

# A long string that contains the necessary Postgres login information
postgres_str = ('postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}').format(username=POSTGRES_USERNAME,
																						 password=POSTGRES_PASSWORD,
																						 ipaddress=POSTGRES_ADDRESS,
																						 port=POSTGRES_PORT,
																						 dbname=POSTGRES_DBNAME)
# Create the connection
cnx = create_engine(postgres_str)
sql_query = """
select ofa.order_id, ofa.attachment_id,  ira.name as file_name , of.name , of.cmr_qty

from order_funnel_attachment as ofa

left join ir_attachment as ira on ira.id = ofa.attachment_id
left join order_funnel as of on ofa.order_id = of.id

where of.name in (select name from order_funnel where (cmr_qty is not null and cmr_qty > 0) and create_date > '2023-03-01' limit 10) 

order by order_id  

"""

attachments_df = pd.read_sql_query(sql=text(sql_query), con=cnx.connect())



attachments_df['final_name'] = attachments_df['name'] + '-'+ attachments_df['attachment_id'].astype(str) + '-'+ attachments_df['file_name']

attachments_df.to_excel('C:\\Users\\vp0421\\Desktop\\!OTHERWORK\\DownloadAttachmentsPython\\attachment_table.xlsx', index=False)

# Loop -------------------------

attachments_df = attachments_df.reset_index()
print(attachments_df)

start_url = 'https://odoo.bunasta.eu/web/content/'
end_url = '?download=true'
dir_path = 'C:\\Users\\vp0421\\Downloads'

for i in range(len(attachments_df['attachment_id'])):
	webbrowser.get('chrome').open_new(start_url + str(attachments_df['attachment_id'][i]) + end_url)

time.sleep(30)

for x in range(len(attachments_df['attachment_id'])):
	os.rename(dir_path + "\\" + attachments_df['file_name'][x], dir_path + "\\" + attachments_df['final_name'][x])







