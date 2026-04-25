CREATE DATABASE IF NOT EXISTS test_db;

USE test_db;

DROP TABLE IF EXISTS products;

CREATE TABLE products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    price DOUBLE
);

INSERT INTO
    products (name, price)
VALUES ('Laptop', 1200.00),
    ('Phone', 600.00),
    ('Tablet', 300.00);