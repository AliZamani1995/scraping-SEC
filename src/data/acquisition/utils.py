"""
This file should contain all required utility functions for the data acquisition step
"""
from typing import Dict, List, Text

import pandas as pd
import requests
import xmltodict
from bs4 import BeautifulSoup
from src.utils import Log

def load_data(
    cik_dict: Dict,
    base_url: Text,
    extend_url: Text,
    headers: Dict
) -> pd.DataFrame:
    """Loads data from the www.sec.gov website into panda dataframe.
    Scraping SEC for inside trading information

    Parameters
    ----------
    cik_dict : Dict
        The dictionary of companies with their CIK code

    base_url: Text
        The url of the website for scraping

    extend_url: Text
        The extension url of the website for scraping

    headers: Dict
        The header for sending request to the website

    Returns
    -------
    pd.DataFrame
        Dataframe containing all of the necessary data scrapped from the website
    """
    final_data_df = pd.DataFrame()

    for cik_name, cik in cik_dict.items():
        Log.info(msg='cik: '+str(cik_name))
        folder_url_list = get_urls(
            url=base_url+extend_url+str(cik),
            headers=headers
        )
        for folder_url in folder_url_list:
            file_url_list = get_urls(
                url=base_url+folder_url,
                headers=headers
            )
            index_file_url = find_index_file(url_list=file_url_list)
            
            xml_url = get_form_4_url(
                url=base_url+index_file_url,
                headers=headers
            )
            if xml_url != None:
                data_df = parse_xml_url(
                    url=base_url+xml_url,
                    headers=headers,
                    cik_name=cik_name
                )
                final_data_df = pd.concat(
                    [data_df, final_data_df],
                    ignore_index=True
                )
    return final_data_df


def get_urls(
    url: Text,
    headers: Dict
) -> List:
    """Gets urls in a page from the www.sec.gov website.

    Parameters
    ----------
    url: Text
        The url of the page for getting urls

    headers: Dict
        The header for sending request to the website

    Returns
    -------
    List
        List containing all of the url of folders
    """
    html_page = requests.get(
        url=url,
        headers=headers
    )
    soup = BeautifulSoup(html_page.content, 'html.parser')
    url_list = [code.get('href') for code in soup.find('table').find_all('a')]
    return url_list


def find_index_file(url_list: List) -> Text:
    """Finds the file which ends with index.html

    Parameters
    ----------
    url_list: List
        The list of url to find index.html file among them

    Returns
    -------
    Text
        The url of the file which ends with index.html
    """
    for url in url_list:
        if 'index.html' == url.split('-')[-1]:
            return url


def get_table_rows(url: Text, headers: Dict) -> List:
    """Gets rows in the table in a page.

    Parameters
    ----------
    url: Text
        The url of the page for getting the rows of the table

    headers: Dict
        The header for sending request to the website

    Returns
    -------
    List
        List containing all of the rows of the table
    """
    html_page = requests.get(
        url=url,
        headers=headers
    )
    soup = BeautifulSoup(html_page.content, 'html.parser')
    rows_list = soup.find('table').find_all('tr')[1:]
    return rows_list


def get_form_4_url(url: Text, headers: Dict) -> Text:
    """Gets xml url of the form 4 in a page.

    Parameters
    ----------
    url: Text
        The url of the page for getting the url of Form 4

    headers: Dict
        The header for sending request to the website

    Returns
    -------
    Text or None
        The url of the Form 4 is exist otherwise None
    """
    table_rows_list = get_table_rows(
        url=url,
        headers=headers
    )
    for row in table_rows_list:
        col_list = row.find_all('td')
        if ('FORM 4' in col_list[1].text or 'form4' in col_list[2].text) and col_list[2].text.split('.')[-1] == 'xml':
            xml_url = col_list[2].find('a').get('href')
            return xml_url
    return None


def parse_xml_url(url: Text, headers: Text, cik_name: Text) -> pd.DataFrame:
    """Parses an xml page.

    Parameters
    ----------
    url: Text
        The url of the page for parsing

    headers: Dict
        The header for sending request to the website

    cik_name: Text
        The name of the company

    Returns
    -------
    pd.DataFrame
        DataFrame containing all of the parsed data
    """
    xml_page = requests.get(
        url=url,
        headers=headers
    )
    data = xmltodict.parse(xml_page.content)
    derivative_data_df = extract_data(
        data=data, derivative_type='derivativeTable', cik_name=cik_name)
    non_derivative_data_df = extract_data(
        data=data, derivative_type='nonDerivativeTable', cik_name=cik_name)
    data_df = pd.concat(
        [derivative_data_df, non_derivative_data_df],
        ignore_index=True
        )
    return data_df


def extract_data(data: Dict, derivative_type: Text, cik_name: Text):
    """Extracts important values from data.

    Parameters
    ----------
    Data: Dict
        The data using for extracting important values

    derivative_type: Text
        The type of derivative which can be 'nonDerivativeTable' or 'derivativeTable'

    Returns
    -------
    pd.DataFrame
        DataFrame containing all of the important values
    """
    data_df = pd.DataFrame()
    if derivative_type in data['ownershipDocument'] and data['ownershipDocument'][derivative_type] != None:
        if derivative_type == 'nonDerivativeTable':
            if 'nonDerivativeTransaction' not in data['ownershipDocument'][derivative_type]:
                return data_df
            else:
                derivatives_data = data['ownershipDocument'][derivative_type]['nonDerivativeTransaction']
        elif derivative_type == 'derivativeTable':
            if 'derivativeTransaction' not in data['ownershipDocument'][derivative_type]:
                return data_df
            else:
                derivatives_data = data['ownershipDocument'][derivative_type]['derivativeTransaction']
        if type(derivatives_data) != list:
            derivatives_data = [derivatives_data]

        for derivative_data in derivatives_data:
            if 'Common Stock' in derivative_data['securityTitle']['value'] and\
            data['ownershipDocument']['issuer']['issuerTradingSymbol']==cik_name:
                try:
                    price = derivative_data['transactionAmounts']['transactionPricePerShare']['value']
                except:
                    price = None

                data_df = data_df.append(
                    pd.Series({
                        'issuerCik': data['ownershipDocument']['issuer']['issuerCik'],
                        'issuerName': data['ownershipDocument']['issuer']['issuerName'],
                        'issuerTradingSymbol': data['ownershipDocument']['issuer']['issuerTradingSymbol'],
                        'securityTitle': derivative_data['securityTitle']['value'],
                        'transactionDate': derivative_data['transactionDate']['value'],
                        'transactionShares': derivative_data['transactionAmounts']['transactionShares']['value'],
                        'transactionPricePerShare': price,
                        'directOrIndirectOwnership': derivative_data['ownershipNature']['directOrIndirectOwnership']['value'],
                        'derivative_type': derivative_type
                    }),
                    ignore_index=True,
                )
    return data_df
