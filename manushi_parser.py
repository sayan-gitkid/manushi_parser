import pandas as pd
import numpy as np
import os
import csv

file_path = "/home/sshakya/Downloads/Audit Report 2075.xlsx"
df = pd.read_excel(file_path, sheet_name='Sheet1', skiprows=1)

# select useful columns
used_cols = [x for x in df.columns.to_list() if 'Unnamed' not in x]

# separate valid data. i.e Amount not null and 0.0 and Qty not null
main_df = df[((~df['Amount'].isna()) & (df['Amount'] != 0.0)) | (~df['Qty'].isna())][used_cols].copy()
main_df['Area'] = main_df['Area'].ffill()

# get index of discarded rows
df_index = df.index
main_df_index = main_df.index
discarded_df = df.loc[df_index.difference(main_df_index)]

curret_dir = os.path.dirname(os.path.abspath(__file__))
discarded_data_path = os.path.join(curret_dir, 'invalid_data')
valid_data_path = os.path.join(curret_dir, 'valid_data')

os.makedirs(discarded_data_path, exist_ok=True)
os.makedirs(valid_data_path, exist_ok=True)

discarded_df.to_csv(discarded_data_path + '/invalid.csv', index=False,
                    encoding='utf-8', na_rep='NA', header=True)

print(main_df.shape)
print(main_df.dtypes)
print(type(np.nan))
imp_cols = ['Code', 'Qty', 'Rate', 'Amount']

for col in imp_cols:
    temp_dp = main_df[main_df[col].isnull()]
    main_df = main_df[~main_df[col].isnull()].copy()
    temp_dp.to_csv(discarded_data_path + '/na_values_' + col + '.csv',
                   index=False, encoding='utf-8', quoting=csv.QUOTE_ALL, na_rep='NA')

main_df.to_csv(valid_data_path + '/valid.csv', index=False, encoding='utf-8', quoting=csv.QUOTE_ALL)

a = 10
