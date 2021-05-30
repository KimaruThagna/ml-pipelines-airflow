import pandas as pd
import requests

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


def get_platinum_customer(): 
    pass


def get_basket_analysis_dataset(): 
    pass


def get_recommendation_engine_dataset(): 
    pass




