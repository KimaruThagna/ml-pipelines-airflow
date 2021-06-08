CREATE TABLE IF NOT EXISTS platinum_customers(
        user_id INTEGER PRIMARY KEY not null,
        product_name VARCHAR(200) not null,
        total_purchase_value FLOAT not null,
        timestamp date not null default CURRENT_DATE
    );