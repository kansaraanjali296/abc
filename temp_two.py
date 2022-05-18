import os
import sqlite3
import warnings
import requests
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
import logging
import pyodbc
from webdriver_manager.chrome import ChromeDriverManager

warnings.simplefilter("ignore")
driver = webdriver.Chrome(ChromeDriverManager().install())

def generate_file_name(bstdc_url):
    file_name = bstdc_url.split('https://')[1].split("/")[0].replace(".", "_")
    return file_name


def create_db(file_name):
    conn = sqlite3.connect(file_name + '.db')
    Cursor = conn.cursor()

    create_db_query = """CREATE TABLE cenexahmedabad_nic_in(Id INTEGER PRIMARY KEY,
                                                            Tender_Notice_No TEXT,
                                                            Tender_Summery TEXT,
                                                            Tender_Details TEXT,
                                                            Bid_deadline_2 TEXT,
                                                            Documents_2 TEXT,
                                                            TenderListing_key TEXT,
                                                            Notice_Type TEXT,
                                                            Competition TEXT,
                                                            Purchaser_Name TEXT,
                                                            Pur_Add TEXT,
                                                            Pur_State TEXT,
                                                            Pur_City TEXT,
                                                            Pur_Country TEXT,
                                                            Pur_Email TEXT,
                                                            Pur_URL TEXT,
                                                            Bid_Deadline_1 TEXT,
                                                            Financier_Name TEXT,
                                                            CPV TEXT,
                                                            scannedImage TEXT,
                                                            Documents_1 TEXT,
                                                            Documents_3 TEXT,
                                                            Documents_4 TEXT,
                                                            Documents_5 TEXT,
                                                            currency TEXT,
                                                            actualvalue TEXT,
                                                            TenderFor TEXT,
                                                            TenderType TEXT,
                                                            SiteName TEXT,
                                                            createdOn TEXT,
                                                            updateOn TEXT,
                                                            Content TEXT,
                                                            Content1 TEXT,
                                                            Content2 TEXT,
                                                            Content3 TEXT,
                                                            DocFees TEXT,
                                                            EMD TEXT,
                                                            OpeningDate TEXT,
                                                            Tender_No TEXT,
                                                            Flag TEXT)"""

    tb_exists = "SELECT name FROM sqlite_master WHERE type='table' AND name='cenexahmedabad_nic_in'"

    if not Cursor.execute(tb_exists).fetchone():
        Cursor.execute(create_db_query)
    # else:
    #     print('Table Already Exists! Now, You Can Insert Date.')
    return conn


def download_pdf(links):
    response = requests.get(links)
    fullname = os.path.join(files_dir, datetime.now().strftime(f"%d%m%Y_%H%M%S%f") + "." + links.rsplit('.', 1)[-1])

    pdf = open(fullname, 'wb')
    pdf.write(response.content)
    pdf.close()
    logging.info("File Downloaded")

    return fullname


def sqlite_and_sql_server_db(Page_Data):
    try:
        que = "INSERT INTO cenexahmedabad_nic_in(Tender_Summery, OpeningDate, Bid_deadline_2, Documents_2, Flag) VALUES (?, ?, ?, ?, 1)"
        conn.executemany(que, Page_Data)
        conn.commit()

        logging.info('SQLite Data Inserted')

        sqlite_rows = conn.execute("SELECT Tender_Summery, OpeningDate, Bid_deadline_2, Documents_2 FROM cenexahmedabad_nic_in WHERE Flag = 1").fetchall()
        if len(sqlite_rows) > 0:
            sql_conn = pyodbc.connect('Driver={SQL Server};'
                                    'Server=192.168.100.153;'
                                    'Database=CrawlingDB;'
                                    'UID=anjali;'
                                    'PWD=anjali@123;')
            sql_cursor = sql_conn.cursor()
            sql_cursor.execute("""IF NOT EXISTS (SELECT name FROM sysobjects WHERE name='cenexahmedabad_nic_in' AND xtype='U') CREATE TABLE cenexahmedabad_nic_in (id int IDENTITY(1,1) PRIMARY KEY, 
                                                                                                                                                                    Tender_Notice_No TEXT,                                                                                                                                
                                                                                                                                                                    Tender_Summery TEXT,
                                                                                                                                                                    Tender_Details TEXT,
                                                                                                                                                                    Bid_deadline_2 TEXT,
                                                                                                                                                                    Documents_2 TEXT,
                                                                                                                                                                    TenderListing_key TEXT,
                                                                                                                                                                    Notice_Type TEXT,
                                                                                                                                                                    Competition TEXT,
                                                                                                                                                                    Purchaser_Name TEXT,
                                                                                                                                                                    Pur_Add TEXT,
                                                                                                                                                                    Pur_State TEXT,
                                                                                                                                                                    Pur_City TEXT,
                                                                                                                                                                    Pur_Country TEXT,
                                                                                                                                                                    Pur_Email TEXT,
                                                                                                                                                                    Pur_URL TEXT,
                                                                                                                                                                    Bid_Deadline_1 TEXT,
                                                                                                                                                                    Financier_Name TEXT,
                                                                                                                                                                    CPV TEXT,
                                                                                                                                                                    scannedImage TEXT,
                                                                                                                                                                    Documents_1 TEXT,
                                                                                                                                                                    Documents_3 TEXT,
                                                                                                                                                                    Documents_4 TEXT,
                                                                                                                                                                    Documents_5 TEXT,
                                                                                                                                                                    currency TEXT,
                                                                                                                                                                    actualvalue TEXT,
                                                                                                                                                                    TenderFor TEXT,
                                                                                                                                                                    TenderType TEXT,
                                                                                                                                                                    SiteName TEXT,
                                                                                                                                                                    createdOn TEXT,
                                                                                                                                                                    updateOn TEXT,
                                                                                                                                                                    Content TEXT,
                                                                                                                                                                    Content1 TEXT,
                                                                                                                                                                    Content2 TEXT,
                                                                                                                                                                    Content3 TEXT,
                                                                                                                                                                    DocFees TEXT,
                                                                                                                                                                    EMD TEXT,
                                                                                                                                                                    OpeningDate TEXT,
                                                                                                                                                                    Tender_No TEXT)""")

            que1 = "INSERT INTO cenexahmedabad_nic_in(Tender_Summery, OpeningDate, Bid_deadline_2, Documents_2) VALUES (?, ?, ?, ?)"
            sql_cursor.executemany(que1, sqlite_rows)
            sql_conn.commit()
            logging.info('SQL Server Data Inserted')
            sql_conn.close()

            conn.execute("UPDATE cenexahmedabad_nic_in SET Flag = 0 WHERE Flag = 1")
            conn.commit()
    except Exception as e:
        print(e)


try:

    bstdc_url = 'https://cenexahmedabad.nic.in/ahmedabad_1/tender.html'

    file_name = generate_file_name(bstdc_url)
    conn = create_db(file_name)

    logging.basicConfig(filename=file_name + '.log', filemode='a', level=logging.DEBUG, format='%(asctime)s %(message)s')
    logging.info('Started Web Scraping')
    logging.info('Program Start')

    files_dir = os.path.expanduser('~') + "\\Documents\\" + "PythonFile\\" + bstdc_url.split('https://')[1].split("/")[0] + "py\\" + "File"
    if os.path.exists(files_dir):
        pass
    else:
        os.makedirs(files_dir)

    driver.get(bstdc_url)

    tr_all_data = driver.find_elements(By.XPATH, value='//*[@class="contecttable"]//tbody//tr')
    tr_all_d = driver.find_elements(By.XPATH, value='//*[@class="contecttable"]//tbody//tr/td[2]/a')
    del tr_all_d[0]
    del tr_all_data[0]
    # del tr_all_data[0:41]

    Page_Data = []
    for tr_all_data, tr_all_d in zip(tr_all_data, tr_all_d):
        try:

            links = tr_all_d.get_attribute('href')

            Tender_Summery = tr_all_data.find_element(By.XPATH, value='./td[2]').text

            OpeningDate = tr_all_data.find_element(By.XPATH, value='./td[3]').text

            Bid_deadline_2 = tr_all_data.find_element(By.XPATH, value='./td[4]').text

            row_exists = "SELECT * FROM cenexahmedabad_nic_in WHERE Tender_Summery='" + Tender_Summery + "' AND OpeningDate='" + OpeningDate + "' AND Bid_deadline_2='" + Bid_deadline_2 + "'"

            if not conn.execute(row_exists).fetchone():

                logging.info("Fresh")
                Documents_2 = download_pdf(links)

                Row_data = [Tender_Summery, OpeningDate, Bid_deadline_2, Documents_2]
                Page_Data.append(Row_data)

            else:
                print("This Tender_Summery, OpeningDate, Bid_deadline_2 is already exist.")
                logging.info("Duplicated")

        except Exception as e:
            print(e)


    sqlite_and_sql_server_db(Page_Data)

    conn.close()
    driver.close()
    logging.info('Proceed Successfully Done')
    print("completed")

except Exception as e:
    print(e)

