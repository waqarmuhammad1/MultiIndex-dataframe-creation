#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pickle
import pandas as pd
from pathlib import Path
from glob import glob
import pygsheets
import operator
from functools import reduce

# In[2]:

sheet_id = '1IlKCUX-8SOFvZpW9o53XHIm5pdorGn-dCJ-lS1QHpnU'

city_df = pd.read_html('https://worldpopulationreview.com/zips/texas')[0]
city_df['County'] = city_df['County'].apply(lambda x: x.replace('County', ''))

# In[3]:


files_list = glob('/home/codexnow/Documents/county_pickle/*.*')
files_list.sort()
county_city = [{x: city_df[city_df['County'].str.contains(r'\b{0}\b'.format(Path(x).stem))]} for x in files_list]

# In[5]:


zip_df = pd.read_csv('zip_codes_data.csv')

# In[6]:


allowed_zips = list(zip_df[zip_df['Type'].str.contains(r'\bStandard\b')]['ZIP Code'])

# In[7]:


allowed_zips = [str(x).strip() for x in allowed_zips]


# In[8]:

'''Create dataframe from dictionary'''

def get_file_df(file_path):
    cc = pickle.load(open(file_path, 'rb'))
    costs = ['150', '200', '350']
    ages = ['1-9', '10-34', '35']
    all_records = []
    records = []
    # Calculate headers values for dataframe from dictionary object
    # This loop will determine that how many columns 150, 200 and 350 are covering from dictionary object
    # Since its a dataset with 4 different 2 different main headers hence the nested loop.
    for cost in costs:
        for age in ages:
            for zip_record in cc[cost][age]:
                for zip_code, zip_data in zip_record.items():
                    cost_length = len(zip_data.columns)
                    header = []

                    header.append(['{0},000'.format(cost)] * cost_length)
                    if age == '35':
                        header.append(['{0}+ Years Old'.format(age)] * cost_length)
                    else:
                        header.append(['{0} Years Old'.format(age)] * cost_length)
                    header.append([zip_code] * cost_length)
                    header.append(list(zip_data.columns))

                    zip_data.columns = header
                    records.append(zip_data)

    # Populate dataframe with the list of dataframes created in the above loops
    me = records[0].copy(deep=True)
    column_name = [x for x in me.columns if 'Company name' in x][0]
    me['Company name'] = me[column_name]
    me.drop(column_name, axis=1, inplace=True)
    column_name = [x for x in me.columns if 'AM' in x][0]
    me['AM'] = me[column_name]
    me.drop(column_name, axis=1, inplace=True)
    me = me[['AM', 'Company name']]
    df_list = []
    for i in range(0, len(records)):
        df2 = records[i]
        column_name2 = [x for x in df2.columns if 'Company name' in x]
        if len(column_name2) > 0:
            column_name2 = column_name2[0]
            df2['Company name'] = df2[column_name2]
            df2.drop(column_name2, axis=1, inplace=True)

            column_name2 = [x for x in df2.columns if 'AM' in x][0]
            df2['AM'] = df2[column_name2]
            df2.drop(column_name2, axis=1, inplace=True)
            try:
                me[list(df2.columns)]
            except:
                df_list.append(df2)
                me = me.merge(df2, on=['Company name', 'AM'])
        else:
            # I needed the average calculations as well so thats why this piece of code is there.
            # No need to add it if you dont need it.
            merged = pd.concat(df_list, axis=1)
            cost = list(df_list[0].columns)[0][0]
            age = list(df_list[0].columns)[0][1]
            brick_columns = [x for x in merged.columns if 'Brick' in x and cost in x and age in x]
            wall_columns = [x for x in merged.columns if 'Wall' in x and cost in x and age in x]
            merged[(cost, age, 'City Average', 'Brick Average')] = merged[brick_columns].mean(axis=1).round(2)
            merged[(cost, age, 'City Average', 'Wall Average')] = merged[wall_columns].mean(axis=1).round(2)
            merged[(cost, age, 'City Average', 'Total Average')] = merged[[(cost, age, 'City Average', 'Brick Average'),
                                                                           (cost, age, 'City Average',
                                                                            'Wall Average')]].mean(axis=1).round(2)
            df = merged.loc[:, ~merged.columns.duplicated()].copy()
            me = me.merge(df[[('Company name', '', '', ''), (cost, age, 'City Average', 'Brick Average'),
                              (cost, age, 'City Average', 'Wall Average'),
                              (cost, age, 'City Average', 'Total Average')]], on=['Company name'])
            df_list = []
    allowed_cols = [x for x in me.columns
                    if any(y in str(x) or
                           'company' in str(x).lower() or
                           'am' in str(x).lower() or
                           'city average' in str(x).lower()
                           for y in allowed_zips)]
    return me[allowed_cols]


# In[9]:

empty_sheets = []

'''This method rearrange the dataframe columns, for some reason if a columns under same header like a column which 
was in 150,000 and 1-9 Years Old isn't next to their relatives pygsheets will just create the same header again. So I 
had to make sure columns which fall under the same header should be together '''


def write_city_to_sheet(city_wise, county_name):
    costs = ['150,000', '200,000', '350,000']
    ages = ['1-9 Years Old', '10-34 Years Old', '35+ Years Old']

    gc = pygsheets.authorize(service_file='credentials.json')
    sh = gc.open_by_key(sheet_id)
    kk = {}
    for city_data in city_wise:
        for key in city_data:
            p_df = city_data[key].copy(deep=True)
            p_df = p_df[['AM', 'Company name']]
            p_df = p_df.loc[:, ~p_df.columns.duplicated()]
            for cost in costs:
                for age in ages:
                    df = city_data[key].copy(deep=True)
                    cols = [x for x in df.columns if
                            (cost in str(x) and age in str(x)) or 'company' in str(x).lower() or 'am' in str(x).lower()]
                    cols = list(set(cols))
                    cols.sort()
                    df = df.loc[:, ~df.columns.duplicated()]
                    df.insert(0, 'Company name', df.pop('Company name'))
                    df.insert(0, 'AM', df.pop('AM'))
                    df2 = df[cols]
                    df2.insert(0, 'Company name', df2.pop('Company name'))
                    df2.insert(0, 'AM', df2.pop('AM'))
                    brick_columns = [x for x in df2.columns if 'Brick' in x and cost in x and age in x]
                    wall_columns = [x for x in df2.columns if 'Wall' in x and cost in x and age in x]
                    df2[(cost, age, 'City Average', 'Brick Average')] = df2[brick_columns].mean(axis=1).round(2)
                    df2[(cost, age, 'City Average', 'Wall Average')] = df2[wall_columns].mean(axis=1).round(2)
                    df2[(cost, age, 'City Average', 'Total Average')] = df2[
                        [(cost, age, 'City Average', 'Brick Average'),
                         (cost, age, 'City Average', 'Wall Average')]].mean(axis=1).round(2)
                    df2 = df2.loc[:, ~df2.columns.duplicated()]
                    p_df = p_df.merge(df2, on=['Company name', 'AM'])
            kk[key] = p_df
    for city_name in kk:
        sheet_name = city_name
        df_to_write = kk[city_name]
        try:
            sh.add_worksheet(sheet_name)
        except:
            sheet_name = '{0}_{1}'.format(city_name, county_name)
            sh.add_worksheet(sheet_name)

        wks = sh.worksheet_by_title(sheet_name)
        wks.set_dataframe(df_to_write, (1, 1), extend=True)
        format_sheet(city_name)


'''This method takes in sheet name and format (merge, center) the headers. So it doesn't look awkward'''


def format_sheet(sheet_name):
    data_to_find = ['150,000', '200,000', '350,000', '1-9 Years Old', '10-34 Years Old', '35+ Years Old',
                    'City Average']
    # sheet_id = '1X5IwcuQsnfT4VCYBCHGonQeJsoPXgj6LFlWwRw75nZQ'
    gc = pygsheets.authorize(service_file='credentials.json')
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet_by_title(sheet_name)

    for to_find in data_to_find:
        found_cells = ws.find(to_find)
        range_data = []

        first_label = found_cells[0].address.label
        last_label = first_label

        range_index = found_cells[0].address.index[1]
        for x in found_cells[1:]:
            if (x.address.index[1] - range_index) == 1:
                range_index = x.address.index[1]
                last_label = x.address.label
            else:
                range_data.append((first_label, last_label))
                rng = ws.get_values(first_label, last_label, returnas='range')
                rng.merge_cells()
                range_index = x.address.index[1]
                first_label = x.address.label
                last_label = x.address.label
        range_data.append((first_label, last_label))
        rng = ws.get_values(first_label, last_label, returnas='range')
        rng.merge_cells()
    for to_find in data_to_find:
        found_cells = ws.find(to_find)
        for cell in found_cells:
            f = cell.link(ws)
            f.horizontal_alignment = pygsheets.custom_types.HorizontalAlignment.CENTER


# In[ ]:


for county_data in county_city:
    for key in county_data:
        file_path = key
        city_info = county_data[key]
        cities = list(city_info['City'].unique())
        city_data = {x: list(city_info[city_info['City'] == x]['Zip Code'].unique()) for x in cities}
        me = get_file_df(key)
        if me is None:
            continue
        temp_dicts = [{x: z} for x in city_data for y in city_data[x]
                      for z in me.columns if str(y) in str(z) or 'company' in str(z).lower() or 'am' in str(z).lower()]

        all_keys = reduce(operator.or_, (d.keys() for d in temp_dicts))
        bar = {key: list(filter(None, [d.get(key) for d in temp_dicts])) for key in all_keys}
        [bar[x].sort() for x in bar]
        city_wise = [{x: me[bar[x]]} for x in bar]
        write_city_to_sheet(city_wise, Path(file_path).stem)
