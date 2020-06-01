from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
import re
import pandas as pd
import os
import time
import pdb

def main():
    url = 'http://phila.maps.arcgis.com/home/item.html?id=4499b7f8631f4ec0af18446f64b15261#data'

    driver = webdriver.Firefox()
    driver.implicitly_wait(30)
    driver.get(url)

    time.sleep(40)
    soup = BeautifulSoup(driver.page_source, 'lxml')

    table = soup.find_all('table')
    #Giving the HTML table to pandas to put in a dataframe object
    df = pd.read_html(str(table))

    col_names = df[0].columns
    df = pd.concat(df)
    df.columns = col_names
    print(df)
if __name__ == '__main__':
    main()