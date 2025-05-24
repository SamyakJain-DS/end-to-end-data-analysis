import pickle
from bs4 import BeautifulSoup
import re
import pandas as pd
import numpy as np
from mysqldatabase import Database
import os

def create_dfs(htmls):
    """
    Generate a dictionary of dfs according to the categories provided in htmls.
    May need to be modified for categories other than "laptops", "mobiles" & "tablets".
    """

    df_dict = {}
    dfs = {}
    for category in htmls.keys():
        if category in ('mobiles', 'tablets'):  # initialise dataframes
            df_dict[category] = {
                'name': [],
                'price': [],
                'spec_score': [],
                'user_rating': [],
                'connectivity': [],
                'cpu': [],
                'ram': [],
                'battery': [],
                'display': [],
                'camera': [],
                'expandable': [],
                'os': [],
                'fm_radio': [],
                'img': [],
                'other': []
            }
        else:
            df_dict[category] = {
                'name': [],
                'price': [],
                'spec_score': [],
                'user_rating': [],
                'cpu1': [],
                'cpu2': [],
                'ram': [],
                'hdd': [],
                'ssd': [],
                'gpu': [],
                'display': [],
                'os': [],
                'warranty': [],
                'img': []
            }

        for brand in htmls[category]:
            # defining some variables
            html = htmls[category][brand]
            soup = BeautifulSoup(html, 'html.parser')
            ins = soup.find_all('div', class_='sm-product has-tag has-features has-actions')
            outs = soup.find_all('div', class_='sm-product has-tag oos has-features has-actions')
            products = ins + outs
            score_bg = 'score rank-{rank}-bg'
            keys = list(df_dict[category].keys())
            keys.remove(
                'name')  # will use this to append nan values to all other columns if no value is found while scraping

            for product in products:
                # scraping information with the help of their tags and attributes
                if product.find('h2').text:
                    df_dict[category]['name'].append(product.find('h2').text)
                else:
                    df_dict[category]['name'].append(np.nan)

                df_dict[category]['price'].append(
                    product.find('span', class_='price').text.replace('₹', '').replace(',', ''))

                for rank in range(1, 6):
                    if product.find('div', class_=score_bg.format(rank=rank)):
                        df_dict[category]['spec_score'].append(int(
                            re.findall(r'\d+', product.find('div', class_=score_bg.format(rank=rank)).text)[0]
                        ))
                        break

                try:
                    if product.find('span', class_='sm-rating'):
                        df_dict[category]['user_rating'].append(float(
                            re.findall(r'\d+(?:\.\d+)?', product.find('span', class_='sm-rating').get('style'))[0]
                        ))
                except Exception as e:
                    print(e, category, brand, product.find('h2').text)

                df_dict[category]['img'].append(product.find('img', class_='sm-img').get('src'))

                if category in ('mobiles', 'tablets'):
                    if (product.find('ul', class_='sm-feat specs')):
                        camera_flag = True
                        ram_flag = True
                        for li in product.find('ul', class_='sm-feat specs').find_all(
                                'li'):  # loop through all list items
                            if 'sim' in li.text.lower():
                                df_dict[category]['connectivity'].append(li.text)
                            elif 'processor' in li.text.lower():
                                df_dict[category]['cpu'].append(li.text)
                            elif (('ram' in li.text.lower()) or ('inbuilt' in li.text.lower()) or (
                                    'rom' in li.text.lower())) and (ram_flag):
                                ram_flag = False
                                df_dict[category]['ram'].append(li.text)
                            elif ('ram' in li.text.lower()) or ('inbuilt' in li.text.lower()) or (
                                    'rom' in li.text.lower()):
                                df_dict[category]['ram'][-1] = (df_dict[category]['ram'][-1] + ', ' + li.text)
                            elif 'mah' in li.text.lower():
                                df_dict[category]['battery'].append(li.text)
                            elif ('inches' in li.text.lower()) or ('px' in li.text.lower()):
                                df_dict[category]['display'].append(li.text)
                            elif (('mp' in li.text.lower()) or ('camera' in li.text.lower())) and (camera_flag):
                                camera_flag = False
                                df_dict[category]['camera'].append(li.text)
                            elif ('mp' in li.text.lower()) or ('camera' in li.text.lower()):
                                df_dict[category]['camera'][-1] = (df_dict[category]['camera'][-1] + ' & ' + li.text)
                            elif 'memory' in li.text.lower():
                                df_dict[category]['expandable'].append(li.text)
                            elif bool(re.search(r'v\d+(\.\d+)?', li.text.lower())):
                                df_dict[category]['os'].append(li.text)
                            elif 'fm' in li.text.lower():
                                df_dict[category]['fm_radio'].append(li.text)
                            elif (bool(re.search(r'[2-5]G', li.text.lower()))) or ('wi-fi' in li.text.lower()):
                                df_dict[category]['other'].append(li.text)
                            else:
                                continue
                else:
                    idx = 0
                    display_flag = True
                    if (product.find('ul', class_='sm-feat specs')):
                        for li in product.find('ul', class_='sm-feat specs').find_all('li'):
                            idx += 1
                            if ' ram' in li.text.lower():
                                df_dict[category]['ram'].append(li.text)
                            elif 'hard disk' in li.text.lower():
                                df_dict[category]['hdd'].append(li.text)
                            elif 'ssd' in li.text.lower():
                                df_dict[category]['ssd'].append(li.text)
                            elif (('inches' in li.text.lower()) or ('pixels' in li.text.lower())) and (display_flag):
                                display_flag = False
                                df_dict[category]['display'].append(li.text)
                            elif ('inches' in li.text.lower()) or ('pixels' in li.text.lower()):
                                df_dict[category]['display'][-1] = (df_dict[category]['display'][-1] + ', ' + li.text)
                            elif 'warranty' in li.text.lower():
                                df_dict[category]['warranty'].append(li.text)
                            elif ' os' in li.text.lower():
                                df_dict[category]['os'].append(li.text)
                            elif (('gpu' in li.text.lower()) or
                                  ('integrated' in li.text.lower()) or
                                  ('graphics' in li.text.lower()) or
                                  ('uhd' in li.text.lower()) or
                                  ('nvidia' in li.text.lower()) or
                                  ('amd radeon' in li.text.lower())):
                                if 'nvidia core' in li.text.lower():
                                    pass
                                else:
                                    df_dict[category]['gpu'].append(li.text)
                            elif idx == 1:
                                df_dict[category]['cpu1'].append(li.text)
                            elif idx == 2:
                                df_dict[category]['cpu2'].append(li.text)
                            else:
                                continue

                for column in keys:
                    if len(df_dict[category][column]) != len(df_dict[category]['name']):
                        df_dict[category][column].append(np.nan)
        dfs[category] = pd.DataFrame(df_dict[category])

    return dfs

os.makedirs("pickle_objects", exist_ok=True)
with open("pickle_objects/final_htmls.pkl", "rb") as h:
    htmls = pickle.load(h)

dfs = create_dfs(htmls)

password = os.environ['SQL_ROOT_PASSWORD']
dbo = Database(username = 'root', password = password, host = 'localhost', database = 'project' )

for category, df in dfs.items():
    dbo.create_table(df, table_name = category + '_uncleaned')
dbo.close()


