import re
import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3

# Base URL template for scraping
BASE_URL = "https://apps.sarpy.gov/CaptureCZ/CAPortal/CAMA/CAPortal/Custom/CZ_RealPropertyPRCPrint27.aspx?Item1={}&Item3=0&SessionGUID=256616a8-0cc4-4e22-beeb-c4c66be5c207"

def get_item_number(url):
    """Extract the item number from the URL to use as a key."""
    item = re.search(r'Item1=(\d+)', url)
    return item.group(1) if item else None

def extract_table_data(soup, table_index, var_text):
    """Extract data from a specific table based on variable text."""
    try:
        data_table = soup.find_all('table')[table_index]
        cell = data_table.find('td', string=f'{var_text} : ')
        return cell.find_next_sibling().text.strip() if cell else None
    except IndexError:
        return None

def extract_data(soup):
    """Extract general property data using a dictionary of table indices and variable texts."""
    data = {}
    var_dict = {
        2: ["Parcel Number", "Situs", "Legal"],
        5: ["Use", "Zoning", "Taxable Value"]
    }
    for table_index, variable_texts in var_dict.items():
        for var_text in variable_texts:
            data[var_text] = extract_table_data(soup, table_index, var_text)
    return pd.DataFrame(data, index=[0])

def extract_physical_info(soup):
    """Extract physical property information from a nested table."""
    try:
        data_table = soup.find_all('table')[22]
        physical_info_table = data_table.find_all('table')[0]
        physical_info_data = {}
        for row in physical_info_table.find_all('tr')[1:]:
            cols = row.find_all('td')
            key = cols[0].text.strip().rstrip(":").replace('.', '')
            value = cols[1].text.strip()
            physical_info_data[key] = value
        year_built_age = physical_info_table.find('td', string='Year Built/Age : ').find_next('td').text.strip()
        physcl_data_df = pd.DataFrame([physical_info_data])
        physcl_data_df[['Year_Built', 'Age']] = year_built_age.split('/')
        physcl_data_df['Year_Built'] = pd.to_numeric(physcl_data_df['Year_Built'], errors='coerce')
        physcl_data_df['Age'] = pd.to_numeric(physcl_data_df['Age'], errors='coerce')
        roll_table = soup.find_all('table')[19]
        roll_year = roll_table.find('td', string=lambda text: text and 'Roll Year' in text).text.split(':')[-1].strip()
        physcl_data_df['roll_year'] = roll_year
        return physcl_data_df
    except (IndexError, AttributeError):
        return pd.DataFrame()

def extract_cost_approach(soup):
    """Extract cost approach valuation data."""
    try:
        data_table = soup.find_all('table')[39]
        value_info = {}
        for row in data_table.find_all('tr'):
            cells = row.find_all('td')
            label = cells[1].text.strip().replace('.', '').rstrip()
            value = cells[2].text.strip().replace('$', '').replace(',', '')
            value_info[label] = value
        return pd.DataFrame([value_info])
    except (IndexError, AttributeError):
        return pd.DataFrame()

def extract_grantee_data(soup):
    """Extract sale history including grantee and grantor data."""
    columns = ["Date", "Book/Page", "Grantor", "Grantee", "Price", "Adj Price"]
    try:
        data_table = soup.find_all('table')[8]
        data = [[col.text.strip() for col in row.find_all('td')][:6] for row in data_table.find_all('tr')[1:]]
        grantee_df = pd.DataFrame(data, columns=columns)
        grantee_df['Date'] = pd.to_datetime(grantee_df['Date'], errors='coerce')
        return grantee_df
    except IndexError:
        return pd.DataFrame()

def merge_data(soup, url):
    """Merge all extracted data into a single DataFrame."""
    default_date = datetime.datetime(1900, 1, 1)
    item = get_item_number(url)
    extracted_data = extract_data(soup)
    cost_approach = extract_cost_approach(soup)
    physcl_data_df = extract_physical_info(soup)
    grantee_df = extract_grantee_data(soup)
    
    physcl_data_df['parcel'] = item
    grantee_df['parcel'] = item
    merged_df = pd.concat([extracted_data, cost_approach, physcl_data_df], axis=1)
    merged_df = pd.merge(merged_df, grantee_df, on='parcel', how='inner')
    
    merged_df['Date'].fillna(default_date, inplace=True)
    merged_df['lookahead_Date'] = merged_df['Date'].shift(1)
    merged_df['lookback_Date'] = merged_df['Date'].shift(-1)
    merged_df['grantor_lived_years'] = merged_df.apply(
        lambda row: 0 if pd.isnull(row['lookahead_Date']) else (row['Date'] - row['lookahead_Date']) / pd.Timedelta(days=365.25), axis=1)
    merged_df['grantee_lived_years'] = merged_df.apply(
        lambda row: 0 if pd.isnull(row['lookback_Date']) else (row['lookback_Date'] - row['Date']) / pd.Timedelta(days=365.25), axis=1)
    
    if not merged_df.empty:
        last_valid_date = merged_df['Date'].dropna().iloc[-1]
        today = datetime.datetime.today()
        merged_df.loc[merged_df['grantee_lived_years'].isna(), 'grantee_lived_years'] = (today - last_valid_date) / pd.Timedelta(days=365.25)
    
    return merged_df

def process_data_to_sql(df):
    """Process DataFrame into a structured SQL table."""
    try:
        conn = sqlite3.connect(':memory:')  # Using SQLite for demonstration; replace with SSMS connection if needed
        df.to_sql('table1', conn, index=False)
        query = """
            SELECT DISTINCT
                "Parcel Number" AS key,
                Situs || ', NEBRASKA' AS address,
                Legal AS legal,
                roll_year,
                Use AS use,
                Zoning AS zoning,
                CAST(REPLACE("Taxable Value", ',', '') AS INTEGER) AS tax_value,
                "Improvement Value" AS imprvmt_value,
                "Land Value" AS land_value,
                "Value per Square Foot" AS value_pr_sq_ft,
                "Quality " AS quality,
                "Condition " AS condition,
                "Arch Type " AS arch_type,
                "Style " AS style,
                "Exterior Wall " AS extr_wall,
                "Floor Area " AS floor_area,
                "Basement Area " AS basement_area,
                "Sub Floor " AS sub_floor,
                "Bedrooms " AS bedroom,
                "Baths " AS baths,
                "Heat Type" AS heat_type,
                "Roof Type" AS roof_type,
                "Plumbing Fixt" AS plumbing,
                Year_Built AS year_built,
                Age AS age,
                Date AS sale_date,
                Grantor AS grantor,
                Grantee AS grantee,
                CAST(REPLACE("Adj Price", ',', '') AS INTEGER) AS adj_price,
                CAST(REPLACE(Price, ',', '') AS INTEGER) AS sale_price,
                CAST(grantor_lived_years AS FLOAT) AS grantor_lived_years,
                CAST(grantee_lived_years AS FLOAT) AS grantee_lived_years
            FROM table1
            WHERE "Taxable Value" IS NOT NULL
              AND "Taxable Value" <> ''
              AND Grantee IS NOT NULL
              AND Grantee <> ''
              AND roll_year = 2024
        """
        final_df = pd.read_sql_query(query, conn)
        final_df.set_index('key', inplace=True)
        conn.close()
        return final_df
    except Exception as e:
        print(f"SQL processing error: {e}")
        return None

# Main scraping loop
result_df = pd.DataFrame()
for i in range(1380000, 1380010):  # Limited range for demonstration
    url = BASE_URL.format(i)
    soup = BeautifulSoup(requests.get(url).content, "html.parser")
    merged_df = merge_data(soup, url)
    final_df = process_data_to_sql(merged_df)
    if final_df is not None and not final_df.empty:
        final_df['i'] = i
        result_df = pd.concat([result_df, final_df])
result_df.to_csv('scraped_property_data.csv', index=True)