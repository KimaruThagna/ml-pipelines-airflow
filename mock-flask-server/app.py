from flask import jsonify
from flask.app import app
from .data_generator import *

@app.route("/products")
def products():
    return jsonify(generate_fake_products)


@app.route("/users")
def products():
    return jsonify(generate_fake_users)



@app.route("/trnsactions")
def products():
    return jsonify(generate_fake_transaction_data)
