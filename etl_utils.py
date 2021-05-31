import pandas as pd
import requests
import numpy as np

'''
The faux data lake is to represent a cloud based storage like s3 or GCS
'''
# modeled as extraction jobs

def pull_user_data(): 
    user_data = requests.get('http://0.0.0.0:5000/users')
    user_df = pd.read_json(user_data.json())
    user_df.to_csv('faux_data_lake/user_df.csv')

def pull_product_data(): 
    product_data = requests.get('http://0.0.0.0:5000/products')
    product_df = pd.read_json(product_data.json())
    product_df.to_csv('faux_data_lake/product_df.csv')

def pull_transaction_data(): 
    transaction_data = requests.get('http://0.0.0.0:5000/transactions')
    transaction_df = pd.read_json(transaction_data.json())
    transaction_df.to_csv('faux_data_lake/transaction_df.csv')

# modeled as transformation jobs

def get_platinum_customer(): 
    '''
    a platinum customer has purchased goods worth over 5000 and store in DB
    '''
    pass


def get_basket_analysis_dataset(): 
    '''
    group by purchase ID and store data
    '''
    transaction_df = pd.read_csv('faux_data_lake/transaction_df.csv')
    transaction_df = transaction_df[['product_id','quantity','purchase_id']]
    # pivot to have purchase ID as index, product IDs as columns and quantity(mean) as the values
    # for basket analysis, one is considering what goes together(cause and effect). So average is a better agg function
    transaction_df.pivot(index='purchase_id', columns='product_id',values='quantity')
    transaction_df.to_csv('faux_data_lake/basket_analysis.csv')


def get_recommendation_engine_dataset(): 
    '''
    group by user ID and send to data lake as dataset
    '''
    transaction_df = pd.read_csv('faux_data_lake/transaction_df.csv')
    transaction_df = transaction_df[['user_id','quantity','product_id']]
    # pivot to have user ID as index, product IDs as columns and quantity(sum) as the values
    # for recommendation engines, it may be critical to know what kind of products are bought in large quantities over time
    transaction_df = pd.pivot_table(transaction_df,index='user_id', 
                   columns='product_id',
                   values='quantity', aggfunc=np.sum)
    
    transaction_df.to_csv('faux_data_lake/recommendation_engine_analysis.csv')
    

# modeled as loading job

def load_platinum_customers_to_db(): 
    pass



