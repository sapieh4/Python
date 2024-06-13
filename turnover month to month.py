
import pandas as pd
from sqlalchemy import create_engine, text
import openpyxl




# connect database and get data--------------------

##########################
#     ERP PROD DB        #
##########################

POSTGRES_ADDRESS = '***'
POSTGRES_PORT = '***'
POSTGRES_USERNAME = '***'  ## CHANGE THIS TO YOUR PANOPLY/POSTGRES USERNAME
POSTGRES_PASSWORD = '***'  ## CHANGE THIS TO YOUR PANOPLY/POSTGRES PASSWORD
POSTGRES_DBNAME = '***'  ## CHANGE THIS TO YOUR DATABASE NAME

# A long string that contains the necessary Postgres login information
postgres_str = ('postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}').format(username=POSTGRES_USERNAME,
                                                                                             password=POSTGRES_PASSWORD,
                                                                                             ipaddress=POSTGRES_ADDRESS,
                                                                                             port=POSTGRES_PORT,
                                                                                             dbname=POSTGRES_DBNAME)
# Create the connection
cnx = create_engine(postgres_str)
sql_query_orders = f'''
WITH MonthlySales AS (
    SELECT
        rc1.name AS sales_depart,
		hr_employee.name as manager,
        of.customer_name,
        TO_CHAR(DATE_TRUNC('month', of.create_date), 'YYYY-MM') AS month,
		CASE
        	WHEN (bdg.name = 'All Partners' or bdg.name = 'NEW EU' or bdg.name ='New EU IM/EX') THEN 'Partners'
        	WHEN (bdg.name ='EEU BY' or bdg.name ='EEU RU') THEN 'SNG'
        	WHEN (bdg.name ='DOP' or bdg.name ='insurance') THEN 'Others'
        	ELSE 'T1'
    	END AS direction_group,
        SUM(offi.total_price) AS monthly_total
    FROM
        order_funnel AS of
    LEFT JOIN
        order_funnel_for_invoice AS offi ON offi.order_id = of.id
	LEFT JOIN
		res_partner AS rp ON rp.id = of.customer_id
	LEFT JOIN
		res_company AS rc1 ON rp.sales_department_id = rc1.id
	LEFT JOIN 
		res_users ON rp.account_manager_id = res_users.id
	LEFT JOIN
		hr_employee ON res_users.id = hr_employee.user_id
	LEFT JOIN
		product_template AS pt ON pt.id = offi.product_tmp_id
	LEFT JOIN
		bunasta_direction_group AS bdg ON bdg.id = pt.direction_id
    WHERE
        offi.total_price IS NOT NULL
        AND of.create_date > '2024-01-01'
    GROUP BY
		rc1.name,
        hr_employee.name,
        of.customer_name,
		direction_group,
        TO_CHAR(DATE_TRUNC('month', of.create_date), 'YYYY-MM')
),
SalesWithLag AS (
    SELECT
        sales_depart,
		manager,
        customer_name,
		direction_group,
        month,
        monthly_total,
        LAG(monthly_total) OVER (
            PARTITION BY sales_depart, customer_name, direction_group
            ORDER BY month
        ) AS previous_month_total
    FROM
        MonthlySales
)
SELECT
    sales_depart,
	manager,
    customer_name,
	direction_group,
    month,
    monthly_total,
	previous_month_total,
    CASE
        WHEN (previous_month_total = 0 or previous_month_total is NULL) AND (monthly_total = 0 or monthly_total is NULL) THEN 0
        WHEN (previous_month_total = 0 or previous_month_total is NULL) AND monthly_total > 0 THEN 1
        WHEN previous_month_total > 0 AND (monthly_total = 0 or monthly_total is NULL) THEN -1        
        ELSE (monthly_total - previous_month_total) / previous_month_total
    END AS percentage_change,
	
	CASE
        WHEN previous_month_total IS NULL THEN NULL
        WHEN previous_month_total = 0 THEN NULL
        ELSE (monthly_total - previous_month_total)
    END AS amount_change
FROM
    SalesWithLag
ORDER BY
    sales_depart,
    customer_name,
    month;


        '''

df = pd.read_sql_query(sql=text(sql_query_orders), con=cnx.connect())

# Assuming `df` is the DataFrame containing the result of the SQL query
# For example, you might load it from a CSV for illustration purposes:
# df = pd.read_csv('monthly_sales_data.csv')

# Creating the pivot table
pivot_table = pd.pivot_table(
    df,
    index=['sales_depart', 'manager',  'customer_name', 'direction_group'],
    values=['monthly_total', 'percentage_change'],
    columns='month',
    aggfunc={'monthly_total': 'sum', 'percentage_change': 'mean'},  # Assuming sum for total and mean for percentage change
    fill_value=0  # Fill NaN values with 0
)

# Flatten the MultiIndex in columns
pivot_table.columns = [f'{val}_{col}' for val, col in pivot_table.columns]

# Reorder columns to have 'monthly_total' and 'percentage_change' in the desired sequence
new_order = []
months = sorted(set(col.split('_')[2] for col in pivot_table.columns))  # Get the unique months
print(months)

for month in months:
    new_order.append(f'monthly_total_{month}')
    new_order.append(f'percentage_change_{month}')

# Select the columns in the new order
pivot_table = pivot_table[new_order]

# Resetting the index to turn MultiIndex into columns
pivot_table.reset_index(inplace=True)

# Format 'percentage_change' columns to percentage format
percentage_change_cols = [col for col in pivot_table.columns if 'percentage_change' in col]

# Convert percentage_change columns to percentage format
for col in percentage_change_cols:
    pivot_table[col] = pivot_table[col].apply(lambda x: f'{x:.2%}')  # Format as percentage

# Format 'monthly_total' columns to currency format
monthly_total_cols = [col for col in pivot_table.columns if 'monthly_total' in col]

# Convert monthly_total columns to currency format
for col in monthly_total_cols:
    pivot_table[col] = pivot_table[col].apply(lambda x: f'€{x:,.2f}')  # Format as currency


# Displaying the pivot table

pivot_table.drop(columns=['percentage_change_2024-01'], inplace=True)

# pivot_table.to_excel('pivot_table.xlsx', index=False)

excel_file = 'pivot_table.xlsx'

# Save the DataFrame to an Excel file and adjust column widths
with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
    pivot_table.to_excel(writer, index=False, sheet_name='Sheet1')
    worksheet = writer.sheets['Sheet1']

    # Adjust column widths
    for i, col in enumerate(pivot_table.columns):
        max_length = max(pivot_table[col].astype(str).map(len).max(), len(col)) + 2
        worksheet.set_column(i, i, max_length)