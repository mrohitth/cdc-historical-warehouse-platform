-- Initialize orders table in operational_db
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    total_amount DECIMAL(10,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    order_status VARCHAR(50) NOT NULL DEFAULT 'pending',
    order_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create index for better query performance
CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_order_date ON orders(order_date);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(order_status);

-- Insert some initial sample data
INSERT INTO orders (customer_id, product_id, quantity, unit_price, order_status) VALUES
(1, 101, 2, 29.99, 'completed'),
(2, 102, 1, 49.99, 'pending'),
(3, 103, 3, 15.99, 'shipped'),
(4, 104, 1, 199.99, 'completed'),
(5, 105, 2, 39.99, 'pending');
