import glob
import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine, text
import openpyxl




def create_df(scrapper_file_path):

    df = pd.read_csv(scrapper_file_path)




    # add info about border

    df['border_num'] = [x.split('/')[-1] for x in df['tinklapis']] #get border code from tinklapis collumn

    borders_table = {
        "Border": ['РПТО "Козловичи"', 'РПТО "Бигосово-1"', 'РПТО "Урбаны"', 'РПТО "Привалка"', 'РПТО "Бенякони"', 'РПТО "Каменный Лог"', 'РПТО "Котловка"', 'РПТО "Бигосово-1"'],
        "border_num": ['9103', '7208', '72', '16412', '16407', '16404', '16401', '7206'],
        "Border country": ['PL', 'LV', 'LV', 'LT', 'LT', 'LT', 'LT', 'LV']
        }
    borders_df = pd.DataFrame(borders_table)

    df = pd.merge(df, borders_df, on ="border_num", how ='left')


    kazahstan_pattern_company = '^[0-9]{3}(A|B|C|D|E|F|G|H|J|K|L|M|N|O|P|Q|R|S|T|U|V|W|X|Y|Z){2}(01|02|17|03|04|05|06|07|08|09|10|11|12|13|14|15|16|20|18|19)$'
    kazahstan_pattern_personal = '^[0-9]{3}(A|B|C|D|E|F|G|H|J|K|L|M|N|O|P|Q|R|S|T|U|V|W|X|Y|Z){3}(01|02|17|03|04|05|06|07|08|09|10|11|12|13|14|15|16|20|18|19)$'

    uzbekistan_pattern_company = '^[0-9]{5}(A|B|C|D|E|F|G|H|J|K|L|M|N|O|P|Q|R|S|T|U|V|W|X|Y|Z){3}'
    uzbekistan_pattern_personal = '^[0-9]{2}(A|B|C|D|E|F|G|H|J|K|L|M|N|O|P|Q|R|S|T|U|V|W|X|Y|Z)[0-9]{2}(A|B|C|D|E|F|G|H|J|K|L|M|N|O|P|Q|R|S|T|U|V|W|X|Y|Z){3}'

    kirgyzstan_pattern_personal = '^0[1-9][0-9]{3}[a-zA-Z]{3}$'
    kirgyzstan_pattern_company = '^0[1-9][0-9]{3}[a-zA-Z]{2}$'

    tajikistan_pattern_personal = '^[0-9]{4}[АВЕКМНОРСТХDJZY]{2}(0[1-8]|10)$'
    tajikistan_pattern_company = '^[0-9]{3}[АВЕКМНОРСТХDJZY]{2}(0[1-8]|10)$'

    turkmenistan_pattern_personal = '^[A-Z]{2}[0-9]{4}(AG|AH|BN|DZ|LB|MR)$'
    turkmenistan_pattern_company = '^[0-9]{4}(AG|AH|BN|DZ|LB|MR)[A-Z]$'

    lithuanian_pattern = '^[A-Z]{3}[0-9]{3}$'

    belarusian_pattern_company = '^[ABEIKMHOPCTX]{2}[0-9]{4}[1234567]$'
    belarusian_pattern_personal = '^[0-9]{4}[ABEIKMHOPCTX]{2}[1234567]$'

    russian_pattern = '^[ABEYKMHOPCTX][0-9]{3}[ABEYKMHOPCTX]{2}(01|101|04|02|102|702|03|103|05|80|180|06|07|08|09|109|10|11|111|82|182|81|181|12|13|113|14|15|16|116|716|17|18|118|19|20|95|21|121|22|122|75|41|23|93|123|193|24|84|88|124|59|159|25|125|725|26|126|27|28|29|30|130|31|32|33|34|134|35|36|136|85|185|37|138|39|91|139|40|42|142|43|44|45|46|47|147|48|49|50|90|150|190|250|550|750|790|51|52|152|252|53|54|154|754|55|155|56|156|57|58|158|60|61|161|761|62|63|163|763|64|164|65|66|96|196|67|68|69|169|70|71|72|172|73|173|74|174|774|76|77|97|99|177|197|199|777|797|799|977|78|98|178|198|92|192|79|83|86|186|87|89|94|188)$'

    turkish_pattern = '^[0-9]{2}[A-Z]{2}[A-Z0-9][0-9]{3}$'

    latvian_pattern = '^[A-Za-z]{2}\d{3,4}$'

    polish_pattern = r'^(' \
                  r'DJ|DL|DB|DW|DBL|DDZ|DGL|DGR|DJA|DJE|DKA|DKL|DLE|DLB|DLU|DLW|DMI|DOL|DOA|DPL|DST|DSR|DSW|DTR|DBA|DWL|DWR|DZA|DZG|DZL|' \
                  r'CB|CG|CT|CW|CAL|CBR|CBY|CCH|CGD|CGR|CIN|CLI|CMG|CNA|CRA|CRY|CSE|CSW|CTR|CTU|CWA|CWL|CZN|' \
                  r'LB|LC|LU|LZ|LBI|LBL|LCH|LHR|LJA|LKS|LKR|LLB|LUB|LLE|LLU|LOP|LPA|LPU|LRA|LRY|LSW|LTM|LWL|LZA|' \
                  r'FG|FZ|FGW|FKR|FMI|FNW|FSL|FSD|FSU|FSW|FWS|FZI|FZG|FZA|' \
                  r'EL|EP|ES|EBE|EBR|EKU|ELA|ELE|ELC|ELW|EOP|EPA|EPJ|EPI|EPD|ERA|ERW|ESI|ESK|ETM|EWI|EWE|EZD|EZG|' \
                  r'KR|KN|KT|KBC|KBR|KCH|KDA|KGR|KRA|KLI|KMI|KMY|KNS|KNT|KOL|KOS|KPR|KSU|KTA|KTT|KWA|KWI|' \
                  r'WB|WA|WD|WE|WU|WF|WH|WW|WI|WJ|WK|WN|WT|WX|WY|WO|WP|WR|WS|WBR|WCI|WG|WGS|WGM|WGR|WKZ|WL|WLI|WLS|WMA|WM|WML|WND|WOS|WOR|WOT|WPI|WPL|WPN|WPP|WPZ|WPY|WPU|WRA|WSI|WSE|WSC|WSK|WSZ|WZ|WWE|WWL|WWY|WZW|WZU|WZY|' \
                  r'OP|OB|OGL|OK|OKL|OKR|ONA|ONY|OOL|OPO|OPR|OST|' \
                  r'RK|RP|RZ|RT|RBI|RBR|RDE|RJA|RJS|RKL|RKR|RLS|RLE|RLU|RLA|RMI|RNI|RPR|RPZ|RRS|RZE|RSA|RST|RSR|RTA|' \
                  r'BI|BL|BS|BAU|BIA|BBI|BGR|BHA|BKL|BLM|BMN|BSE|BSI|BSK|BSU|BWM|BZA|' \
                  r'GD|GA|GS|GSP|GBY|GCH|GCZ|GDA|GKA|GKS|GKW|GLE|GMB|GND|GPU|GSL|GST|GSZ|GTC|GWE|' \
                  r'SB|SY|SH|SC|SD|SG|SJZ|SJ|SK|SM|SPI|SL|SRS|SR|SI|SO|SW|ST|SZ|SZO|SBE|SBI|SBL|STY|SCI|SCZ|SGL|SKL|SLU|SMI|SMY|SPS|SRC|SRB|STA|SWD|SZA|SZY|' \
                  r'TK|TBU|TJE|TKA|TKI|TKN|TOP|TOS|TPI|TSA|TSK|TST|TSZ|TLW|' \
                  r'NE|NO|NBA|NBR|NDZ|NEB|NEL|NGI|NGO|NOG|NIL|NKE|NLI|NMR|NNI|NNM|NOE|NOL|NOS|NPI|NSZ|NWE|' \
                  r'PK|PN|PKO|PL|PO|PCH|PCT|PGN|PGS|PGO|PJA|PKA|PKE|PKL|PKN|PKS|PKR|PLE|PMI|PNT|POB|POS|POT|PP|PPL|PZ|POZ|PRA|PSL|PSZ|PSE|PSR|PTU|PWA|PWL|PWR|PZL|' \
                  r'ZK|ZS|ZSW|ZBI|ZCH|ZDR|ZGL|ZGY|ZGR|ZKA|ZKL|ZKO|ZLO|ZMY|ZPL|ZPY|ZSL|ZST|ZSZ|ZSD|ZWA' \
                  r')[A-Z0-9]{5,6}$'


    df['plate_country'] = df['numeris'].apply(
        lambda x: 'Lithuanian' if re.match(lithuanian_pattern, x)
            else 'Polish' if re.match(polish_pattern, x)
            else 'Latvian' if re.match(latvian_pattern, x)
            else 'Russian' if re.match(russian_pattern, x)
            else 'Belarusian company' if re.match(belarusian_pattern_company, x)
            else 'Belarusian personal' if re.match(belarusian_pattern_personal, x)
            else 'Kazahstan personal' if re.match(kazahstan_pattern_company, x)
            else 'Kazahstan company' if re.match(kazahstan_pattern_personal, x)
            else 'Uzbekistan personal' if re.match(uzbekistan_pattern_personal, x)
            else 'Uzbekistan company' if re.match(uzbekistan_pattern_company, x)
            else 'Kirgyzstan personal' if re.match(kirgyzstan_pattern_personal, x)
            else 'Kirgyzstan company' if re.match(kirgyzstan_pattern_company, x)
            else 'Tajikistan personal' if re.match(tajikistan_pattern_personal, x)
            else 'Tajikistan company' if re.match(tajikistan_pattern_company, x)
            else 'Turkmenistan personal' if re.match(turkmenistan_pattern_personal, x)
            else 'Turkmenistan company' if re.match(turkmenistan_pattern_company, x)
            else 'Turkish' if re.match(turkish_pattern, x)
            else "Other")




    # sql queries

    unique_scrapper_plates = df['numeris'].unique()
    unique_scrapper_plates_text = "('" + "', '".join(unique_scrapper_plates) + "')"


    # connect database and get data--------------------

    ##########################
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
    select 
    ptl.plate_number as order_plate_number, 
    rp.name as order_carrier, 
    of.customer_name as order_customer_payer,  
    MAX(of.date_of_request) as order_date
    
    from order_funnel as of 
    
    left join order_funnel_for_invoice as offi on offi.order_id = of.id
    left join bunasta_direction_group as pdg on offi.product_direction_id = pdg.id
    left join partner_transport_line as ptl on ptl.id = of.truck_plate_number_id
    left join res_partner as rp on rp.id = of.carrier_id
    left join res_company as rc2 on rp.our_company_id = rc2.id
    
    
    where  ptl.plate_number in ''' + unique_scrapper_plates_text + '''  and of.date_of_request > (NOW() - INTERVAL '10 DAY') and pdg.name = 'EU'
    
    group by 1,2,3
    
    
        '''

    orders_query = pd.read_sql_query(sql=text(sql_query_orders), con=cnx.connect())

    sql_query_transport_lines = f'''
    select ptl.plate_number as tl_plate_number ,
    
    rp.name as tl_carrier, rp.signed_contract_id as carrier_contract_id,
    --rc.name as carrier_our_company, res_country.name as carrier_country, 
    --rp.vat as carrier_vat, rp.reg_code as carrier_code,
    
    rp_payer.name as payer, 
    --rc_payer.name as payer_our_company, rcp.name as payer_country, 
    rp_payer.signed_contract_id as payer_contract_id,
    
    general_payer.name as carrier_general_payer
    
    from 
    
    public.partner_transport_line as ptl
    
    --carrier
    left join res_partner as rp on ptl.partner_id = rp.id
    left join res_company as rc on rp.our_company_id = rc.id
    left join res_country on rp.country_id = res_country.id
    --payer
    left join res_partner as rp_payer on ptl.payer_id = rp_payer.id
    left join res_company as rc_payer on rp_payer.our_company_id = rc_payer.id
    left join res_country as rcp on rp_payer.country_id = rcp.id
    
    --general payer for carrier
    left join res_partner as general_payer on rp.bill_payer_id = general_payer.id
    
    where ptl.plate_number in ''' + unique_scrapper_plates_text + '''
    
        '''

    transport_lines_query = pd.read_sql_query(sql=text(sql_query_transport_lines), con=cnx.connect())


    last_order_query = f'''
    
    select last_order_id_table.plate_number as last_order_truck_plate, order_funnel.order_done_time, order_funnel.customer_name as last_order_payer, 
    res_partner.signed_contract_id as last_order_payer_contract, total_orders.total_orders_qty as truck_total_orders
    
    
    from
        (
        select ptl.plate_number, MAX(of.id) as last_order_id
        from order_funnel as of
        left join partner_transport_line as ptl on ptl.id = of.truck_plate_number_id
        where of.stage_id in (4,5,6)
        and ptl.plate_number in ''' + unique_scrapper_plates_text + '''
    
        group by 1
        )
    as last_order_id_table
    
    left join order_funnel on order_funnel.id = last_order_id_table.last_order_id
    left join res_partner on res_partner.id = order_funnel.customer_id
    left join 
        (select truck_plate_number_id , COUNT(DISTINCT(id)) as total_orders_qty from order_funnel where stage_id in (4,5,6) group by 1) as total_orders 
        on order_funnel.truck_plate_number_id = total_orders.truck_plate_number_id
    '''
    last_order_query = pd.read_sql_query(sql=text(last_order_query), con=cnx.connect())




    # External DB from excel file--------------------------------------------------------------
    external_db = pd.read_excel(r'C:\Truckplates\External_DB.xlsx', sheet_name='Sheet1')

    df = df.merge(orders_query, how='left', left_on='numeris', right_on='order_plate_number')
    df = df.merge(transport_lines_query, how='left', left_on='numeris', right_on='tl_plate_number')
    df = df.merge(external_db, how='left', left_on='numeris', right_on='db_plate_number')
    df = df.merge(last_order_query, how='left', left_on='numeris', right_on='last_order_truck_plate')

    df['is_our_order?'] = df['order_plate_number'].apply(
        lambda x: 'YES' if pd.notna(x)
        else "NO")



    # Add conditional collumn - recognize type of contact-----------------------------------------------------------------------------------------
    order_plate_number = df['order_plate_number']
    tl_truckplate = df['tl_plate_number']
    tl_carrier = df['tl_carrier']
    contract = df['payer_contract_id']
    ex_db_truckplate = df['db_plate_number']
    db_type = df['DB']
    # List of conditions
    conditions = [
          (order_plate_number.notnull())
        , (order_plate_number.isnull()) & (tl_carrier.notnull())
        , (order_plate_number.isnull()) & (tl_carrier.isnull()) & (ex_db_truckplate.notnull())
        , (order_plate_number.isnull()) & (tl_carrier.isnull()) & (ex_db_truckplate.isnull())
    ]
    # List of values to return
    choices  = [
          "Order"
        , "TL"
        , "DB"
        , "No info"
    ]
    # create a new column in the DF based on the conditions
    df["Info_type"] = np.select(conditions, choices, None)

    # Add conditional collumn by payer------------------------------------------------------------------------------------------
    info_type = df["Info_type"]
    order_carrier = df['order_carrier']
    tl_carrier = df['tl_carrier']
    tl_carrier_contract = df['carrier_contract_id']
    order_customer_payer = df['order_customer_payer']
    carrier_general_payer = df['carrier_general_payer']
    last_order_payer = df ['last_order_payer']
    last_order_payer_contract = df['last_order_payer_contract']



    # List of conditions
    conditions = [

        ((info_type == 'Order') & (order_carrier == order_customer_payer) & carrier_general_payer.isnull()),
        ((info_type == 'Order') & (order_carrier != order_customer_payer) & carrier_general_payer.isnull()),
        ((info_type == 'Order') & (order_carrier != order_customer_payer) & (order_customer_payer != carrier_general_payer) & (order_carrier != carrier_general_payer)),
        ((info_type == 'Order') & (order_carrier != order_customer_payer) & (order_customer_payer == carrier_general_payer)),
        ((info_type == 'Order') & (order_carrier == order_customer_payer) & (order_customer_payer != carrier_general_payer)),
        # DB leads
        (info_type == 'DB'),
        # Leads FV/Carrier
        ((info_type == 'TL') & (tl_carrier_contract.isnull()) & (carrier_general_payer.isnull()) & (tl_carrier != last_order_payer) & (last_order_payer_contract.isnull())),
        # Leads
        ((info_type == 'TL') & (tl_carrier_contract.isnull()) & (carrier_general_payer.isnull()) & (tl_carrier == last_order_payer) & (last_order_payer_contract.isnull())),

        ((info_type == 'TL') & (tl_carrier_contract.isnull()) & (carrier_general_payer.isnull()) & (tl_carrier != last_order_payer) & (last_order_payer_contract.notnull())),

        ((info_type == 'TL') & (tl_carrier_contract.isnull()) & (carrier_general_payer != tl_carrier) & (tl_carrier == last_order_payer) & (last_order_payer_contract.isnull())),
        ((info_type == 'TL') & (tl_carrier_contract.isnull()) & (carrier_general_payer != tl_carrier) & (carrier_general_payer == last_order_payer) & (last_order_payer_contract.notnull())),

        ((info_type == 'TL') & (tl_carrier_contract.isnull()) & (carrier_general_payer != tl_carrier) & (carrier_general_payer != last_order_payer) & (tl_carrier != last_order_payer) & (last_order_payer_contract.isnull())),
        ((info_type == 'TL') & (tl_carrier_contract.isnull()) & (carrier_general_payer != tl_carrier) & (carrier_general_payer != last_order_payer) & (tl_carrier != last_order_payer) & (last_order_payer_contract.notnull())),

        # Upsell
        ((info_type == 'TL')  & (tl_carrier_contract.notnull()) & (carrier_general_payer.isnull()) & (tl_carrier == last_order_payer)),

        ((info_type == 'TL') & (tl_carrier_contract.notnull()) & (carrier_general_payer.isnull()) & (tl_carrier != last_order_payer) & (last_order_payer_contract.isnull())),
        ((info_type == 'TL') & (tl_carrier_contract.notnull()) & (carrier_general_payer.isnull()) & (tl_carrier != last_order_payer) & (last_order_payer_contract.notnull())),

        ((info_type == 'TL') & (tl_carrier_contract.notnull()) & (carrier_general_payer.notnull()) & (tl_carrier == last_order_payer) & (last_order_payer_contract.notnull())),
        ((info_type == 'TL') & (tl_carrier_contract.notnull()) & (carrier_general_payer.notnull()) & (carrier_general_payer == last_order_payer) & (last_order_payer_contract.notnull())),
        ((info_type == 'TL') & (tl_carrier_contract.notnull()) & (carrier_general_payer.notnull()) & (tl_carrier != last_order_payer) & (carrier_general_payer != last_order_payer) & (last_order_payer_contract.isnull())),
        ((info_type == 'TL') & (tl_carrier_contract.notnull()) & (carrier_general_payer.notnull()) & (tl_carrier != last_order_payer) & (carrier_general_payer != last_order_payer) & (last_order_payer_contract.notnull()))


        ]

    # List of values to return
    choices  = [
        # Order
        "order payer is order carrier",
        "order payer isn't order carrier",
        "order payer isn't order carrier and isn't contract payer",
        "order payer isn't order carrier and is contract payer",
        "order payer is order carrier and isn't contract payer",
        # DB leads
        "DB leads",
        # LEADS FV/CARRIER
        "Lead carrier or/and lead payer",
        # Leads carrier
        "Lead carrier or new payer or upsell payer",
        "Lead carrier, upsell payer or lead new payer",
        "Lead carrier or upsell payer",
        "Lead carrier or upsell payer",
        "Lead carrier, upsell payer or lead new payer",
        "Lead carrier or upsell payer",

        #Upsell
        "Upsell carrier, upsell payer or lead new payer",
        "Upsell carrier or lead payer",
        "Upsell carrier, upsell payer or lead new payer",
        "Upsell carrier or upsell payer",
        "Upsell carrier or upsell payer",
        "Upsell carrier, upsell payer or lead new payer",
        "Upsell carrier or payer or lead new payer"
    ]


    # create a new column in the DF based on the conditions
    df["Cathegory"] = np.select(conditions, choices, None)

    # drop not needed collumns--------------------------------------------------------------------------------------------------------------
    columns_to_delete = ['tinklapis', 'priemimas is eiles', 'border_num']
    df = df.drop(columns=columns_to_delete)



    # create group ---------------------------------------------------------------------------------------------------------------------------------------------
    conditions = [
        (df['Info_type'] == 'Order'),
        (df['Cathegory'] == 'DB leads'),
        (df['Cathegory'] == 'Lead carrier or/and lead payer'),
        (df['Cathegory'].isin([
            "Lead carrier or new payer or upsell payer",
            "Lead carrier, upsell payer or lead new payer",
            "Lead carrier or upsell payer",
            "Lead carrier or upsell payer",
            "Lead carrier, upsell payer or lead new payer",
            "Lead carrier or upsell payer"
        ])),
        (df['Cathegory'].isin([
            "Upsell carrier, upsell payer or lead new payer",
            "Upsell carrier or lead payer",
            "Upsell carrier, upsell payer or lead new payer",
            "Upsell carrier or upsell payer",
            "Upsell carrier or upsell payer",
            "Upsell carrier, upsell payer or lead new payer",
            "Upsell carrier or payer or lead new payer"
        ])),
        (df['Info_type'] == 'No info')
    ]

    values = ['current_clients', 'DB_leads', 'leads_fv_carrier', 'leads_carrier', 'upsell', 'no_info_clients']

    # Add new column 'Group' based on conditions
    df['Group'] = np.select(conditions, values, default='Other')



    return df
