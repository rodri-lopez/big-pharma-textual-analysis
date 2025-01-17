## download EDGAR filings
from logging.handlers import DEFAULT_SOAP_LOGGING_PORT
import time
import pandas as pd
from pdfkit import from_url, from_file
import re
import time
from selenium import webdriver
from bs4 import BeautifulSoup
import sys
import os
import random
import traceback

def convert_url_to_pdf(url, pdf_path, year):
    try:
        from_url(url, pdf_path)
        print(f"PDF generated and saved at {pdf_path}")
    except Exception as e:
        print(f"{year} - PDF generation failed: {e}")

def convert_file_to_pdf(filepath, pdf_path, year):
    print(filepath)
    try:
        with open (filepath) as f:
            print(f)
            from_file(f, pdf_path)
        print(f"PDF generated and saved at {pdf_path}")
    except Exception as e:
        print(f"{year} - PDF generation failed: {e}")

def print_dict(dict):
    try:
        for key, _ in dict.items():
            print(key)
        print()
    except AttributeError:
        print("Not a dictionary")
        print(dict)
    
def get_html_link(link: str) -> str:
    driver = webdriver.Chrome()
    driver.get(link)
    html = driver.page_source
    time.sleep(1.0)
    driver.quit()

    soup = BeautifulSoup(html, 'html.parser')
    try:
        table = soup.find_all('table')[0]

        rows = table.find_all('tr')[1:]
        
        pattern = re.compile(r'^10-K.*$')
        for row in rows:
            cell = row.find('td', attrs={'scope':'row'}, string=pattern)
            if cell:
                desired_row = row
                break
    
        report_elem = desired_row.find('a')
        report_url = report_elem.get('href')
        print(report_url)
        return report_url
    
    except Exception as e:
        print("error")
        traceback.print_exc()
        print(soup)
        
    except report_elem is None:
        print("no link")
        return None

def extract_pdf(url: str):
    return

def extraction(dfs: list[tuple[str, pd.DataFrame]]):
    for df_tuple in dfs:
        for _, row in df_tuple[1].iterrows():
            landing_page = row['Filings URL']
            year = row['Reporting date']
            form_type = row['Form type']
            ticker = df_tuple[0]
            folder = ticker + '/'
            filepath = folder+ticker+'_'+year+'_'+form_type+'.pdf' # "<TICKER>/<TICKER>_<YEAR>_<TYPE>.pdf"

            if os.path.isfile(filepath):
                print(f'{filepath} exists')
                continue

            try:
                report_url = "sec.gov" + get_html_link(landing_page)
                sleep_time = random.uniform(2,8)
                time.sleep(sleep_time)
                convert_url_to_pdf(report_url, filepath, year)
                sleep_time = random.uniform(1,2)
            except Exception as e:
                print(f"Unable to extract {report_url}")
    return

# get the index landing page for the year's annual filing
# scrape the html for the link to the actual html for the 10-k filing
# pass this link to the
def main():
    
    url = "https://www.sec.gov/Archives/edgar/data/0001551152/000155115224000011/0001551152-24-000011-index.htm"
    url1 = "https://www.sec.gov/Archives/edgar/data/14272/000104746903009157/0001047469-03-009157-index.htm"
    # report_url = "sec.gov" + get_html_link(url1)
    # convert_url_to_pdf(report_url, "test.pdf")

    if sys.argv[1] == 't':
        tickers = sys.argv[2:]
        dataframes = []
        for ticker in tickers:
            folder = ticker +"/"
            csv_filepath = os.path.join(folder, "EDGAR_"+ticker+"_LANDINGPAGES.csv") #<TICKER>/EDGAR_<TICKER>_LANDINGPAGES.csv

            if os.path.isfile(csv_filepath):
                df = pd.read_csv(csv_filepath, sep=',')
                df = df.dropna()
                df_filtered = df[df['Form type'].str.contains('10-K')]
                df_filtered.loc[:, 'Reporting date'] = df_filtered['Reporting date'].str.split('-', expand=True)[0]
                dataframes.append((ticker, df_filtered))
            else:
                print(f"CSV {csv_filepath} not found in {folder}")
        
        extraction(dfs=dataframes)
    
    if sys.argv[1] == 'l':
        urls = sys.argv[2:]
        for url in urls:
            year_ticker = input(f"Enter: <YEAR> <TICKER> for:\n{url}\n").split(sep=' ')
            if len(year_ticker) == 2:
                year, ticker = year_ticker
                folder = ticker + '/'
                filepath = folder+ticker+'_'+year+'_10-K.pdf' # "<TICKER>/<TICKER>_<YEAR>_<TYPE>.pdf"
                convert_url_to_pdf(url, filepath, year)
            else:
                print("Provide year and ticker")
                break
    
    if sys.argv[1] == 'f':
        files = sys.argv[2:]
        for file in files:
            year_ticker = input(f"Enter: <YEAR> <TICKER> for:\n{url}\n").split(sep=' ')
            if len(year_ticker) == 2:
                year, ticker = year_ticker
                folder = ticker + '/'
                filepath = folder+ticker+'_'+year+'_10-K.pdf' # "<TICKER>/<TICKER>_<YEAR>_<TYPE>.pdf"
                convert_file_to_pdf(file, filepath, year)
            else:
                print("Provide year and ticker")
                break
    


if __name__=="__main__":
    main()