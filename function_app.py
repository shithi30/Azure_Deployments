# import - Azure
import azure.functions as func
import logging
# import - script/function
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build
from bs4 import BeautifulSoup
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from pretty_html_table import build_table
import random
from datetime import datetime, timedelta
import time
import os
import json
# import - Selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select

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
    html_msg["Subject"] = "Home Flyer Deals: " + time.strftime("%d-%b-%y")
    html_msg["From"] = "Shithi Maitra"
    html_msg["To"] = ", ".join(recivr_email)

    # send
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(sender_email, os.getenv("EMAIL_PASS"))
        server.sendmail(sender_email, recivr_email, html_msg.as_string())

    ## logic ends
    logging.info("Python timer trigger function executed.")
    
# job template - 02
@app.timer_trigger(schedule = "0 0 0 31 2 *", arg_name = "myTimer", run_on_startup = False, use_monitor = True) # never trigger
def cluster_activator(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due: logging.info("The timer is past due!")
    ## logic starts

    # pref.
    chrome_options = Options()
    chrome_options.add_argument("no-sandbox")
    chrome_options.add_argument("headless")
    chrome_options.add_argument("disable-dev-shm-usage")

    # open window
    driver = webdriver.Chrome(options = chrome_options)
    driver.implicitly_wait(10)

    # notebook
    driver.get("https://community.cloud.databricks.com/?o=924599453726095#notebook/4004368016220956")

    # login
    driver.find_element(By.ID, "login-email").send_keys("shithi30@outlook.com")
    driver.find_element(By.ID, "login-password").send_keys(os.getenv("DATABRICKS_PASS") + "\n")

    # run
    driver.find_element(By.XPATH, ".//button[@data-testid='notebook-run-all-button']").click()

    # close window
    driver.close()

    ## logic ends
    logging.info("Python timer trigger function executed.")

@app.timer_trigger(schedule = "0 */6 * * * *", arg_name = "myTimer", run_on_startup = False, use_monitor = True)
def visa_appt_checker(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due: logging.info("The timer is past due!")
    ## logic starts

    # pref.
    chrome_options = Options()
    chrome_options.add_argument("headless")
    chrome_options.add_argument("no-sandbox")
    chrome_options.add_argument("disable-dev-shm-usage")
    chrome_options.add_argument("user-agent='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 Safari/537.36'")

    # window
    driver = webdriver.Chrome(options = chrome_options)
    driver.implicitly_wait(3 * 4)
    achains = ActionChains(driver)

    # url
    driver.maximize_window()
    driver.get("https://ais.usvisa-info.com/en-ca/niv/users/sign_in")

    # user
    elem = driver.find_element(By.ID, "user_email")
    elem.send_keys(os.getenv("PORTAL_USER"))

    # pass
    elem = driver.find_element(By.ID, "user_password")
    elem.send_keys(os.getenv("PORTAL_PASS"))

    # terms
    elem = driver.find_element(By.ID, "policy_confirmed")
    achains.move_to_element(elem).click().perform()

    # submit
    elem = driver.find_element(By.NAME, "commit")
    achains.move_to_element(elem).click().perform()

    # continue
    elem = driver.find_element(By.LINK_TEXT, "Continue")
    elem.click()

    # reschedule 1
    elem = driver.find_element(By.LINK_TEXT, "Reschedule Appointment")
    elem.click()

    # reschedule 2
    time.sleep(3)
    elems = driver.find_elements(By.LINK_TEXT, "Reschedule Appointment")
    elems[1].click()

    # acc.
    date_df = pd.DataFrame(columns = ["consulate", "closest_date", "report_time"])
    consulates = ["Calgary", "Halifax", "Montreal", "Ottawa", "Quebec City", "Toronto", "Vancouver"]

    # consulate
    for consulate in consulates: 
        elem = Select(driver.find_element(By.NAME, "appointments[consulate_appointment][facility_id]"))
        elem.select_by_visible_text(consulate)
        time.sleep(3)
        
        # calendar
        elem = driver.find_element(By.NAME, "appointments[consulate_appointment][date]")
        try: elem.click()
        except: continue
        
        # soup
        for i in range(0, 24):
            soup = BeautifulSoup(driver.page_source, "html.parser").find_all("td", attrs = {"data-handler": "selectDay"})
            # availability
            available_on_month = len(soup)
            if available_on_month > 0: break
            # next month
            elem = driver.find_element(By.XPATH, ".//a[@title='Next']")
            elem.click()

        # exit calendar
        elem = driver.find_element(By.ID, "appointments_consulate_address")
        elem.click()
        time.sleep(3)
        
        # record
        if available_on_month == 0: continue
        closest_date = datetime.strptime(soup[0]["data-year"] + "-" + str(int(soup[0]["data-month"]) + 1) + "-" + soup[0].get_text(), "%Y-%m-%d").strftime("%Y-%m-%d")
        date_df = pd.concat([date_df, pd.DataFrame([[consulate, closest_date, (datetime.now() - timedelta(hours = 4)).strftime("%Y-%m-%d %I:%M %p")]], columns = date_df.columns)], ignore_index = True)

    # see & filter
    logging.info("\n%s", date_df.to_string())
    date_df = date_df[date_df["closest_date"] < "2025-12-08"]

    # email - from, to, body
    sender_email = "shithi30@gmail.com"
    receiver_email = ["maitra.shithi.aust.cse@gmail.com", "shithi30@outlook.com", "Purnabchowdhury@gmail.com"]
    body = '''Please find the earliest posted empty slots by consulates.''' + build_table(date_df, "green_dark", font_size = "12px", text_align = "left") + '''Thanks,<br>Shithi Maitra<br>Ex Asst. Manager, CS Analytics<br>Unilever BD Ltd.<br>'''

    # email - object
    html_msg = MIMEText(body, "html")
    html_msg["Subject"], html_msg["From"], html_msg["To"] = "US Visa Appt.", "Shithi Maitra", ""

    # email - send
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(sender_email, os.getenv("EMAIL_PASS"))
        if date_df.shape[0] > 0: server.sendmail(sender_email, receiver_email, html_msg.as_string())

    ## logic ends
    logging.info("Python timer trigger function executed.")