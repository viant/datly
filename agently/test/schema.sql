-- DDL: Customer Table
CREATE TABLE customer (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  email VARCHAR(100),
  phone VARCHAR(20),
  billing_address TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- DDL: Invoice Table
CREATE TABLE invoice (
  id INT AUTO_INCREMENT PRIMARY KEY,
  customer_id INT NOT NULL,
  invoice_number VARCHAR(50) UNIQUE NOT NULL,
  issue_date DATE NOT NULL,
  due_date DATE,
  status ENUM('draft', 'issued', 'paid', 'overdue', 'canceled') DEFAULT 'draft',
  total_amount DECIMAL(10,2) NOT NULL,
  currency VARCHAR(3) DEFAULT 'USD',
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  FOREIGN KEY (customer_id) REFERENCES customer(id)
);

-- DDL: Invoice Item Table
CREATE TABLE invoice_item (
  id INT AUTO_INCREMENT PRIMARY KEY,
  invoice_id INT NOT NULL,
  description TEXT NOT NULL,
  quantity INT DEFAULT 1,
  unit_price DECIMAL(10,2) NOT NULL,
  total DECIMAL(10,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
  FOREIGN KEY (invoice_id) REFERENCES invoice(id)
);

-- DDL: Payment Table
CREATE TABLE payment (
  id INT AUTO_INCREMENT PRIMARY KEY,
  invoice_id INT NOT NULL,
  amount_paid DECIMAL(10,2) NOT NULL,
  payment_date DATE NOT NULL,
  method ENUM('card', 'bank_transfer', 'cash', 'paypal') NOT NULL,
  reference VARCHAR(100),
  FOREIGN KEY (invoice_id) REFERENCES invoice(id)
);

-- DML: Insert Customers
INSERT INTO customer (name, email, phone, billing_address)
VALUES
('Alice Smith', 'alice@example.com', '+123456789', '123 Main St'),
('Bob Johnson', 'bob@example.com', '+987654321', '456 Maple Ave');

-- DML: Insert Invoices
INSERT INTO invoice (customer_id, invoice_number, issue_date, due_date, status, total_amount)
VALUES
(1, 'INV-001', '2024-07-01', '2024-07-15', 'issued', 1500.00),
(2, 'INV-002', '2024-07-03', '2024-07-17', 'draft', 2500.00);

-- DML: Insert Invoice Items
INSERT INTO invoice_item (invoice_id, description, quantity, unit_price)
VALUES
(1, 'Consulting services', 10, 100.00),
(1, 'Software license', 1, 500.00),
(2, 'Design work', 20, 125.00);

-- DML: Insert Payment
INSERT INTO payment (invoice_id, amount_paid, payment_date, method, reference)
VALUES
(1, 1000.00, '2024-07-05', 'bank_transfer', 'TXN12345');