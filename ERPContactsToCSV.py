import pandas as pd
from sqlalchemy import create_engine, text


##########################
#     ERP PROD DB        #
##########################

POSTGRES_ADDRESS = '***'
POSTGRES_PORT = '***'
POSTGRES_USERNAME = '***' ## CHANGE THIS TO YOUR PANOPLY/POSTGRES USERNAME
POSTGRES_PASSWORD = '***' ## CHANGE THIS TO YOUR PANOPLY/POSTGRES PASSWORD
POSTGRES_DBNAME = '***' ## CHANGE THIS TO YOUR DATABASE NAME

# A long string that contains the necessary Postgres login information
postgres_str = ('postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}').format(username=POSTGRES_USERNAME,password=POSTGRES_PASSWORD,ipaddress=POSTGRES_ADDRESS,port=POSTGRES_PORT,dbname=POSTGRES_DBNAME)
# Create the connection
cnx = create_engine(postgres_str)
query_companies = """
select 
rs.id , 
rs.name,
rs.reg_code,
rs.vat,
pp.unique_code, 
rs.target_segment,
public.res_partner_group.name as group,
public.customer_status.name as customer_status,
rs.block_id,
rs.blacklist_id,
rs.our_company_id,
rc2.name as our_company, 
rs.sales_department_id,
rs.email,
rc1.name as sales_dep,
public.hr_employee.name as Manager,
rs.country_id,
public.res_country.name as country,

rc_phone.name as country_phone,
rc_phone.phone_code as country_phone_code, 
rs.phone,

rc_mobile.name as country_mobile,
rc_mobile.phone_code as country_mobile_code, 
rs.mobile,

rs.create_date , 
rs.first_order_date, 
rs.total_vehicle_owned, 
rs.boarders_vehicles_qty


from public.res_partner as rs

left join public.res_company as rc1 on rs.sales_department_id = rc1.id
left join public.res_company as rc2 on rs.our_company_id = rc2.id
left join public.res_country on rs.country_id = public.res_country.id
left join public.customer_status on rs.customer_status_id = public.customer_status.id
left join public.res_partner_group on rs.partner_group_id = public.res_partner_group.id
left join public.res_users on rs.account_manager_id = public.res_users.id
left join public.hr_employee on public.res_users.id = public.hr_employee.user_id
left join public.product_pricelist as pp ON rs.id = pp.related_company_id
left join res_country as rc_phone on phone_country_id = rc_phone.id
left join res_country as rc_mobile on mobile_country_id = rc_mobile.id

where rs.active = 'true' and rs.company_type = 'company'
                                               
"""


companies_df = pd.read_sql_query(sql=text(query_companies), con=cnx.connect())


companies_df.to_csv('\\\\192.168.1.111\\Bendri\\Damian S\\companies.csv', index=False)

query_contacts = """
select

rp.id,
rp.parent_id , 
rp.name, 
rp.type, 
rp.job_position, 
rp.decision_maker, 
rp.lang, 

rc_phone.name as country_phone,
rc_phone.phone_code as country_phone_code, 
rp.phone ,

rc_mobile.name as country_mobile,
rc_mobile.phone_code as country_mobile_code, 
rp.mobile,

rc_mobile2.name as country_mobile2,
rc_mobile2.phone_code as country_mobile2_code, 
rp.mobile2,

rp.email , 
rp.email2, 
rp.newsletter_ok, 
rp.participate_ok,
rp.is_company


from 
public.res_partner as rp

left join res_country as rc_phone on phone_country_id = rc_phone.id
left join res_country as rc_mobile on mobile_country_id = rc_mobile.id
left join res_country as rc_mobile2 on mobile2_country_id = rc_mobile2.id

where rp.is_company = false and parent_id is not null

"""

contacts_df = pd.read_sql_query(sql=text(query_contacts), con=cnx.connect())


contacts_df.to_csv('\\\\192.168.1.111\\Bendri\\Damian S\\contacts.csv', index=False)
