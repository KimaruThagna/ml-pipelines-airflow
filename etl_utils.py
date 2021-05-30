import pandas as pd
import requests

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
    transaction_df.to_csv('faux_data_lake/basket_analysis.csv')


def get_recommendation_engine_dataset(): 
    '''
    group by user ID and send to data lake as dataset
    '''
    transaction_df = pd.read_csv('faux_data_lake/transaction_df.csv')
    transaction_df.to_csv('faux_data_lake/recommendation_engine_analysis.csv')

# modeled as loading job

def load_platinum_customers_to_db(): 
    pass


