import pandas as pd

orders = pd.read_csv('data_e_commerce/orders.csv')
sales = pd.read_csv('data_e_commerce/sales.csv')

merged_df = orders.merge(sales, on='order_id')

merged_df.to_csv('data_e_commerce/orders_sales.csv', index='False')