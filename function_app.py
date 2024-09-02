# import - Azure
import azure.functions as func
import logging
# import - script/function
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build
import smtplib
from email.mime.text import MIMEText
from pretty_html_table import build_table
import pandas as pd
import random
import time
import os
import json

# Function App obj.
app = func.FunctionApp()

# job template - 01
@app.timer_trigger(schedule = "0 40 14 * * *", arg_name = "myTimer", run_on_startup = False, use_monitor = True) 
def flyer_notifier(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due: logging.info("The timer is past due!")

    ## logic starts

    # credentials
    creds = service_account.Credentials.from_service_account_info(json.loads(os.getenv("BIGQUERY_KEYS_JSON")))
    client = bigquery.Client(credentials = creds, project = creds.project_id)

    # all items
    query = '''select * from dbt_smaitra.landing_grocery_flyer_items order by id'''
    all_df = client.query(query).result().to_dataframe()

    # household items
    query = '''select household_item, flyer_item, sku, offer_price, platform, string(report_time) report_time from dbt_smaitra.serving_grocery_flyer_items'''
    pick_df = client.query(query).result().to_dataframe()

    # top-03 items
    query = '''
    select *
    from 
        (select household_item `Household Item`, flyer_item `Flyer Item`, offer_price `Price`, platform `Platform`, row_number() over(partition by household_item order by offer_price asc) `Rank`
        from `dbt_smaitra.serving_grocery_flyer_items`
        where 
            offer_price is not null
            and platform in('RealCanadian', 'Freshco', 'NoFrills')
        ) tbl1
    where `Rank` < 4
    order by `Household Item`, `Rank`
    '''
    top_df = client.query(query).result().to_dataframe()

    ## ETL

    # credentials
    SERVICE_ACCOUNT_INFO = json.loads(os.getenv("GSHEET_KEYS_JSON"))
    SAMPLE_SPREADSHEET_ID = "1Fokcum9d__mAxw8PEN_djL34UL9l5Uq8j5LCPwjAE9Y"
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

    # APIs
    creds = service_account.Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO, scopes = SCOPES)
    service = build("sheets", "v4", credentials = creds)
    sheet = service.spreadsheets()

    # update - all items
    clear = sheet.values().clear(spreadsheetId = SAMPLE_SPREADSHEET_ID, range = "All Items").execute()
    reqst = sheet.values().update(spreadsheetId = SAMPLE_SPREADSHEET_ID, range = "'All Items'!A1", valueInputOption = "USER_ENTERED", body = {"values": [all_df.columns.values.tolist()] + all_df.fillna("").values.tolist()}).execute()

    # update - household items 
    clear = sheet.values().clear(spreadsheetId = SAMPLE_SPREADSHEET_ID, range = "Pick Items").execute()
    reqst = sheet.values().update(spreadsheetId = SAMPLE_SPREADSHEET_ID, range = "'Pick Items'!A1", valueInputOption = "USER_ENTERED", body = {"values": [pick_df.columns.values.tolist()] + pick_df.fillna("").values.tolist()}).execute()

    ## update - top-03 items

    # from, to, body
    sender_email = "shithi30@gmail.com"
    recivr_email = ["shithi30@outlook.com", "purnabchowdhury@gmail.com"] if time.strftime("%A") in ["Saturday", "Sunday"] else ["shithi30@outlook.com"]
    body = '''
    Dear housemate,<br><br>
    Summarized below are, today's best household flyer deals (full list of relevant household offers, <a href="https://docs.google.com/spreadsheets/d/1Fokcum9d__mAxw8PEN_djL34UL9l5Uq8j5LCPwjAE9Y/edit?gid=1326928732#gid=1326928732">here</a>). All flyer items can be found <a href="https://docs.google.com/spreadsheets/d/1Fokcum9d__mAxw8PEN_djL34UL9l5Uq8j5LCPwjAE9Y/edit?gid=0#gid=0">here</a>.
    ''' + build_table(top_df, random.choice(["green_dark", "red_dark", "blue_dark", "grey_dark", "orange_dark"]), font_size = "12px", text_align = "left") + '''
    Statistics presented here reflect data of ON's top-10 grocery stores. This is an auto-notifier, deployed on and triggered from <i>MS Azure Function Apps</i>.<br><br>
    Thanks,<br>
    Shithi Maitra<br>
    Ex Asst. Manager, CS Analytics<br>
    Unilever BD Ltd.<br>
    '''

    # email obj.
    html_msg = MIMEText(body, "html")
    html_msg["Subject"] = "Home Flyer Deals: "  + time.strftime("%d-%b-%y")
    html_msg["From"] = "Shithi Maitra"
    html_msg["To"] = ", ".join(recivr_email)

    # send
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(sender_email, os.getenv("EMAIL_PASS"))
        server.sendmail(sender_email, recivr_email, html_msg.as_string())

    ## logic ends
    logging.info("update - Python timer trigger function executed.")

# job template - 02
@app.timer_trigger(schedule = "0 0 13 * * *", arg_name = "myTimer", run_on_startup = False, use_monitor = True) 
def sample_daily_runner(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due: logging.info("The timer is past due!")

    ## logic starts
    print("Good morning! It is now UTC " + time.strftime("%d-%b-%y, %I:%M %p"))
    ## logic ends

    logging.info("Python timer trigger function executed.")