from flask import jsonify, Flask
from data_generator import *

app = Flask(__name__)

@app.route("/products")
def products():
    return jsonify(generate_fake_products())


@app.route("/users")
def users():
    return jsonify(generate_fake_users())



@app.route("/transactions")
def transactions():
    return jsonify(generate_fake_transaction_data())


app.run(host="0.0.0.0", port=5000)