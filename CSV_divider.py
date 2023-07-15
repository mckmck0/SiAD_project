import pandas as pd
import os
from tabulate import tabulate

# All necessary paths
os.makedirs(r'SiAD/Resources', exist_ok=True)
path = r'C:\Users\miko2\PycharmProjects\SiAD\Resources\\'
file_path = r"C:\Users\miko2\PycharmProjects\SiAD\Resources\CriteoSearchData"

# Simple table-like output of DataFrame
pdtabulate = lambda df: tabulate(df, headers=colnames, tablefmt='psql')


colnames = ['Sale', 'SalesAmountInEuro', 'time_delay_for_conversion',
            'click_timestamp', 'nb_clicks_1week', 'product_price',
            'product_age_group', 'device_type', 'audience_id',
            'product_gender', 'product_brand', 'product_category(1)',
            'product_category(2)', 'product_category(3)', 'product_category(4)',
            'product_category(5)', 'product_category(6)', 'product_category(7)',
            'product_country', 'product_id', 'product_title',
            'partner_id', 'user_id']

# Dataset sample to see its structure
data_sample = pd.read_csv(file_path,
                          engine='python', header=None, names=colnames, sep=r'\t', nrows=100)
print("Imported the sample")
print(pdtabulate(data_sample.head()))

# Importing the whole dataset in chunks to optimize the importing process
# And creating a csv files for each partner_id
chunk_size = 50000
for chunk in pd.read_csv(file_path, sep=r'\t', chunksize=chunk_size,
                         header=None, names=colnames, engine='python'):
    for partner_id, df in chunk.groupby('partner_id'):
        file = os.path.join(path + 'partner_id_' + str(partner_id) + '.csv')
        df.to_csv(file, mode='a', header=None, na_rep='N/A', index=False)

print("Imported the full dataset")
