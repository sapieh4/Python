
import smtplib
from email.message import EmailMessage
from datetime import date
from datetime import timedelta
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text


yesterday_date = date.today() - timedelta(days = 1)
print(yesterday_date)
yesterday_date_formated = yesterday_date.strftime("%d.%m.%Y")
date_time = datetime.now()

EMAIL_ADDRESS = 'reporting@bunasta.eu'
EMAIL_PASSWORD = '$rIn3hPB_roiUY7'

script_running_df = pd.read_excel('Script running list.xlsx')


try:
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
	sql_query = f"""

	select

	of.order_done_time at time zone 'utc' at time zone 'Europe/Moscow' as order_done_time,
	of.name as Name ,
	bos.name as Stages,
	of.date_of_request,
	ptl.plate_number as Plate_number_of_truck,
	of.trailer_plate_number as Plate_number_of_trailer,
	rp_carrier.name as Carrier,
	rp_customer.name as Customer_Payer,
	cs.name as Customer_status,
	of.email,
	bc.display_name as Border_crossing,
	wo.name as Service,
	wo.lrn_number,
	wo.mrn_number,
	wo.service_code,
	wo.cargo_value,
	wo.warranty_eur as Warranty,
	wo.codes_qty as Number_of_codes,
	wo.excisable_goods,
	wo.service_price,
	wo_bcrm.name as Work_order_cancel_reason,
	wo.detailed_cancel_reason_name as Work_order_detailed_cancel_reason,
	of_bcrm.name as Order_main_cancel_reason,
	of.detailed_cancel_reason_name as Order_detailed_cancel_reason


	from order_funnel as of

	left join bunasta_order_stage as bos on of.stage_id = bos.id
	left join partner_transport_line as ptl on ptl.id = of.truck_plate_number_id
	left join res_partner as rp_carrier on rp_carrier.id = of.carrier_id
	left join res_partner as rp_customer on rp_customer.id = of.customer_id
	left join customer_status as cs on rp_customer.customer_status_id = cs.id
	left join border_crossing as bc on of.border_crossing_id = bc.id
	left join funnel_work_order as wo on wo.order_funnel_id = of.id
	left join bunasta_canceled_reason_main as wo_bcrm on wo_bcrm.id = wo.main_cancel_reason_id
	left join bunasta_canceled_reason_main as of_bcrm on of_bcrm.id = of.main_cancel_reason_id

	where of.email = 'transit@nastapro.ru'

	and (wo.main_cancel_reason_id IS null OR wo.main_cancel_reason_id IN (1, 6, 7, 8, 9, 10, 11, 13) )

	and of.order_done_time at time zone 'utc' at time zone 'Europe/Moscow' < CURRENT_DATE and of.order_done_time at time zone 'utc' at time zone 'Europe/Moscow' > (CURRENT_DATE - 1)

	order by of.order_done_time , of.name

	"""

	report = pd.read_sql_query(sql=text(sql_query), con=cnx.connect())



	report.to_excel(f'report.xlsx', index=False)

	excel_file = 'report.xlsx'


	# send email ------------------------------------------------------



	To_contacts = ['damian.sapieha@bunasta.eu' ]
	Cc_contacts = [ ]

	#'marina.brazinskiene@bunasta.eu'

	msg = EmailMessage()
	msg['Subject'] = f'Отчет BUNASTA VIM {yesterday_date_formated}'
	msg['From'] = EMAIL_ADDRESS
	msg['To'] = To_contacts
	msg['Cc'] = Cc_contacts

	# attachment = MIMEApplication(open('report.xlsx', 'rb').read())
	# # attachment.add_header('Content-Disposition','attachment','report.xlsx')
	# msg.attach(attachment)

	msg.add_alternative(f"""\
	<!DOCTYPE html>
	<html>
		<body>
			<p>Отчет за {yesterday_date_formated} в приложении.  </p>



			<p>Писмо отправлено автоматически. В случае вопросов просим обращаться в it@bunasta.eu

			</p>
		</body>
	</html>
	""", subtype='html')



	with open(excel_file, 'rb') as f:
		file_data = f.read()
		msg.add_attachment(file_data, maintype="application", subtype="xlsx", filename=excel_file)


	with smtplib.SMTP('mail.bunasta.eu') as smtp:
		smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
		smtp.send_message(msg)

	print("ok")

	#Add row to Script running list
	script_running_df.loc[len(script_running_df)] = [date_time, 1]
	script_running_df.to_excel('Script running list.xlsx', index=False)


except Exception as e:

	try:
		# send email ------------------------------------------------------
		error_msg = EmailMessage()
		error_msg['Subject'] = f'ERROR - Отчет BUNASTA VIM {yesterday_date_formated}'
		error_msg['From'] = EMAIL_ADDRESS
		error_msg['To'] = 'damian.sapieha@bunasta.eu'
		error_msg.add_alternative(f"""\
		<!DOCTYPE html>
		<html>
			<body>
				<p>{str(e)}</p>
			</body>
		</html>
		""", subtype='html')

		with smtplib.SMTP('mail.bunasta.eu') as smtp:
			smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
			smtp.send_message(error_msg)

		print("error")
	except:
		print("email error")

	# Add row to Script running list
	script_running_df.loc[len(script_running_df)] = [date_time, 0]
	script_running_df.to_excel('Script running list.xlsx', index=False)