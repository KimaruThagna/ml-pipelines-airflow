from faker import Faker
from random import randint

faker = Faker()

# generate fake data to be used as ecommerce transaction data

def generate_fake_users():
    users = []
    for i in range(30):
        users.append(
            {
                "user_id":i,
                "name":faker.name(),
                "address":faker.address(),
                "email":faker.email(),
            }
        )
    return users
 
def generate_fake_products():
    products = []
    for i in range(50):
         products.append(
             {
                 "product_name":f'product{faker.name()}',
                 "product_id":i,
                 "product_description":faker.text(40),
                 "price":randint(100,1000)
                 
             }
         )
    return products
         
         
         
def generate_fake_transaction_data():
    transaction_data = []
    for i in range(300):
        transaction_data.append(
             {
            "user_id":randint(1,20),
            "purchase_id":randint(1,10), # allow repitition
            "product_id": randint(1,10),
            "quantity":randint(3,7),
            }
        )
    return transaction_data


