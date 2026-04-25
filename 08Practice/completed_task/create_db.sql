CREATE DATABASE IF NOT EXISTS training_dw;

USE training_dw;

DROP TABLE IF EXISTS orders;

DROP TABLE IF EXISTS customers;

CREATE TABLE customers (
    customer_id INT PRIMARY KEY AUTO_INCREMENT,
    full_name VARCHAR(100) NOT NULL,
    city VARCHAR(80) NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    order_id INT PRIMARY KEY AUTO_INCREMENT,
    customer_id INT NOT NULL,
    order_date DATE NOT NULL,
    amount_usd DECIMAL(10, 2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
);

USE training_dw;

INSERT INTO
    customers (full_name, city)
VALUES ('Alice Johnson', 'Kyiv'),
    ('Bob Smith', 'Lviv'),
    ('Chris Lee', 'Kyiv'),
    ('Daria Ivanova', 'Odesa');

INSERT INTO
    orders (
        customer_id,
        order_date,
        amount_usd,
        status
    )
VALUES (
        1,
        '2026-01-05',
        120.50,
        'PAID'
    ),
    (
        1,
        '2026-01-20',
        75.00,
        'PAID'
    ),
    (
        2,
        '2026-01-11',
        200.00,
        'CANCELLED'
    ),
    (
        2,
        '2026-02-01',
        50.00,
        'PAID'
    ),
    (
        3,
        '2026-02-10',
        99.99,
        'PAID'
    ),
    (
        4,
        '2026-02-15',
        300.00,
        'PAID'
    );