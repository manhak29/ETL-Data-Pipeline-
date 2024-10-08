CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    age INT,
    country VARCHAR(50),
    email VARCHAR(100),
    join_date DATE,
    purchase_amount DECIMAL(10, 2),
    active BOOLEAN
);
