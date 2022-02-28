import pandas as pd
import sqlite3
import os

db_loc = 'data/last_year_compare.sqlite'
user_list_dir = 'data/user_lists'
last_year_dir = 'data/user_lists_old'
last_year = 2021

if __name__ == '__main__':
    old_files = os.listdir(last_year_dir)
    try:
        os.remove(db_loc)
    except OSError as e:
        print('could not delete db file')
        print(e)

    for file in old_files:
        name, year = file.replace('.csv', '').split('_')

        # year doesn't match - ignore it
        if int(year) != last_year:
            continue

        # no list for this year - ignore it
        current_file = f'{name}.csv'

        if current_file not in os.listdir(user_list_dir):
            continue

        old_df = pd.read_csv(f'{last_year_dir}/{file}')
        old_df['Rank'] = old_df.index + 1
        old_df['Year'] = int(last_year)
        old_df['Name'] = name
        new_df = pd.read_csv(f'{user_list_dir}/{current_file}')
        new_df['Rank'] = new_df.index + 1
        new_df['Year'] = int(last_year) + 1
        new_df['Name'] = name

        try:
            conn = sqlite3.connect(db_loc)

            old_df.to_sql('ranks', conn, if_exists='append', index=False)
            new_df.to_sql('ranks', conn, if_exists='append', index=False)
        finally:
            conn.close()
