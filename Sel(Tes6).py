# import requests
# import pandas as pd
# from bs4 import BeautifulSoup
# from selenium import webdriver
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# import psycopg2
# from sqlalchemy import create_engine
# from sqlalchemy.exc import SQLAlchemyError
# import argparse

# def login_to_screener(email, password):
#     options = webdriver.ChromeOptions()
#     options.add_argument('headless')
#     driver = webdriver.Chrome(options=options)
#     login_url = "https://www.screener.in/login/?"
#     driver.get(login_url)
#     WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, 'username'))).send_keys(email)
#     driver.find_element(By.NAME, 'password').send_keys(password)
#     driver.find_element(By.XPATH, "//button[@type='submit']").click()
#     if driver.current_url == "https://www.screener.in/dash/":
#         print("Login successful")
#         return driver
#     else:
#         print("Login failed")
#         return None

# def scrape_reliance_data(driver):
#     search_url = "https://www.screener.in/company/RELIANCE/consolidated/"
#     driver.get(search_url)
#     WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'profit-loss')))
#     soup = BeautifulSoup(driver.page_source, 'html.parser')
#     table1 = soup.find('section', {'id': 'profit-loss'})
#     table = table1.find('table')
#     headers = [th.text.strip() or f'Column_{i}' for i, th in enumerate(table.find_all('th'))]
#     rows = table.find_all('tr')
#     print("Extracted Headers:", headers)
#     row_data = []
#     for row in rows[1:]:
#         cols = row.find_all('td')
#         cols = [col.text.strip() for col in cols]
#         if len(cols) == len(headers):
#             row_data.append(cols)
#         else:
#             print(f"Row data length mismatch: {cols}")
#     df = pd.DataFrame(row_data, columns=headers)
#     if not df.empty:
#         df.columns = ['Narration'] + df.columns[1:].tolist()
#     df = df.reset_index(drop=True)
#     print(df.head())
#     return df

# def save_to_postgres(df, table_name, db, user, password, host, port):
#     engine = create_engine(f"postgresql://{user}:{password}@{host}/{db}", connect_args={'port': port})
#     try:
#         df.to_sql(table_name, con=engine, if_exists='replace', index=False)
#         print("Data saved to Postgres")
#     except SQLAlchemyError as e:
#         print(f"Error: {e}")
#     finally:
#         engine.dispose()

# if __name__ == "__main__":
#     parser = argparse.ArgumentParser()
#     parser.add_argument("--email", default="darshan.patil@godigitaltc.com")
#     parser.add_argument("--password", default="Darshan123")
#     parser.add_argument("--table_name", default="financial_data")
#     parser.add_argument("--db", default="Task6")
#     parser.add_argument("--user", default="Darshan")
#     parser.add_argument("--pw", default="Darshan123")
#     parser.add_argument("--host", default="192.168.3.43")
#     parser.add_argument("--port", default="5432")
#     args = parser.parse_args()
#     driver = login_to_screener(args.email, args.password)
#     if driver:
#         df = scrape_reliance_data(driver)
#         if df is not None:
#             save_to_postgres(df, args.table_name, args.db, args.user, args.pw, args.host, args.port)
#         driver.quit()


from bs4 import BeautifulSoup as bs
import pandas as pd
from sqlalchemy import create_engine
import logging
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Set up Selenium WebDriver
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

# Web scraping
url = 'https://screener.in/company/RELIANCE/consolidated/'
logging.info(f"Fetching data from URL: {url}")
driver.get(url)
soup = bs(driver.page_source, 'html.parser')
data = soup.find('section', id="profit-loss")

if data is not None:
    tdata = data.find("table")
    if tdata is not None:
        table_data = []
        for row in tdata.find_all('tr'):
            row_data = []
            for cell in row.find_all(['th', 'td']):
                row_data.append(cell.text.strip())
            table_data.append(row_data)

        # Convert the scraped table data to a DataFrame
        df_table = pd.DataFrame(table_data)
        df_table.iloc[0, 0] = 'Section'
        df_table.columns = df_table.iloc[0]
        df_table = df_table.iloc[1:, :-2]

        # Transpose the DataFrame to have columns as periods and rows as metrics
        df_table = df_table.set_index('Section').transpose()

        # Reset index after transpose and add an 'id' column
        df_table.reset_index(inplace=True)
        df_table.rename(columns={'index': 'Period'}, inplace=True)
        df_table['id'] = range(1, len(df_table) + 1)

        # Rearrange columns to put 'id' at the beginning
        columns = ['id'] + [col for col in df_table.columns if col != 'id']
        df_table = df_table[columns]

        # Identify and clean numeric data
        for col in df_table.columns[2:]:  # Skip 'id' and 'Period' columns
            if df_table[col].str.isnumeric().all():
                df_table[col] = df_table[col].str.replace(',', '').apply(pd.to_numeric, errors='coerce')
            elif '%' in df_table[col].astype(str).iloc[0]:  # Check if '%' is present
                df_table[col] = df_table[col].str.replace(',', '').str.replace('%', '/100').apply(eval)

        # Log and print the cleaned and transposed DataFrame
        logging.info("Cleaned and transposed DataFrame with 'id' column:")
        print(df_table)

        # Load data to Postgres
        db_host = "192.168.3.43"
        db_name = "Task6"
        db_user = "Darshan"
        db_password = "Darshan123"
        db_port = "5432"
        engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
        # Set the column names
        df_table.columns = [
            'id',
            '0',
            'Sales +',
            'Expenses +',
            'Operating Profit',
            'OPM %',
            'Other Income +',
            'Interest',
            'Depreciation',
            'Profit before tax',
            'Tax %',
            'Net Profit +',
            'EPS in Rs',
            'Dividend Payout %'
        ]

        # Write the DataFrame to the database
        df_table.to_sql('profit_loss_data', engine, if_exists='replace', index=False)
        logging.info("Data loaded to PostgreSQL")

        # Use the existing PostgreSQL connection
        connection = engine.raw_connection()
        cursor = connection.cursor()

        # List the current columns in the table to verify names
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'profit_loss_data';
        """)
        columns = cursor.fetchall()
        logging.info("Columns in 'profit_loss_data' table:")
        for column in columns:
            print(column)

        # Rename columns one by one with error handling
        rename_queries = [
            """ALTER TABLE profit_loss_data RENAME COLUMN "Sales +" TO sales;""",
            """ALTER TABLE profit_loss_data RENAME COLUMN "0" TO month;""",
            """ALTER TABLE profit_loss_data RENAME COLUMN "Expenses +" TO expenses;""",
            """ALTER TABLE profit_loss_data RENAME COLUMN "Operating Profit" TO operating_profit;""",
            """ALTER TABLE profit_loss_data RENAME COLUMN "OPM %" TO operating_profit_margin;""",
            """ALTER TABLE profit_loss_data RENAME COLUMN "Other Income +" TO other_income;""",
            """ALTER TABLE profit_loss_data RENAME COLUMN "Interest" TO interest;""",
            """ALTER TABLE profit_loss_data RENAME COLUMN "Dividend Payout %" TO dividend_payout_ratio;"""
        ]
 
        for query in rename_queries:
            try:
                cursor.execute(query)
                connection.commit()
                logging.info(f"Successfully executed query: {query}")
            except Exception as e:
                logging.error(f"Error with query: {query}\n{e}")
 
        # Close cursor and connection
        cursor.close()
        connection.close()
        logging.info("Data transformed and connections closed")
 
else:
    logging.error("No data found at the given URL or no Profit-Loss section available")
