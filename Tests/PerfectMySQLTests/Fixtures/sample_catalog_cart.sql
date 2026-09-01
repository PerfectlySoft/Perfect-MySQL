CREATE TABLE catalog_categories (
	id BIGINT NOT NULL PRIMARY KEY,
	name VARCHAR(100) NOT NULL
);

CREATE TABLE catalog_products (
	id BIGINT NOT NULL PRIMARY KEY,
	category_id BIGINT NOT NULL,
	sku VARCHAR(40) NOT NULL,
	name VARCHAR(120) NOT NULL,
	price DECIMAL(10,2) NOT NULL,
	active TINYINT(1) NOT NULL,
	created_at DATETIME NOT NULL,
	INDEX idx_catalog_products_category (category_id),
	INDEX idx_catalog_products_active_price (active, price)
);

CREATE TABLE catalog_variants (
	id BIGINT NOT NULL PRIMARY KEY,
	product_id BIGINT NOT NULL,
	sku VARCHAR(40) NOT NULL,
	color VARCHAR(40) NOT NULL,
	size VARCHAR(20) NOT NULL,
	inventory INT NOT NULL,
	INDEX idx_catalog_variants_product (product_id),
	INDEX idx_catalog_variants_color (color)
);

CREATE TABLE catalog_customers (
	id BIGINT NOT NULL PRIMARY KEY,
	email VARCHAR(160) NOT NULL,
	display_name VARCHAR(120) NOT NULL,
	created_at DATETIME NOT NULL,
	UNIQUE KEY idx_catalog_customers_email (email)
);

CREATE TABLE catalog_carts (
	id BIGINT NOT NULL PRIMARY KEY,
	customer_id BIGINT NOT NULL,
	status VARCHAR(20) NOT NULL,
	created_at DATETIME NOT NULL,
	INDEX idx_catalog_carts_customer (customer_id),
	INDEX idx_catalog_carts_status (status)
);

CREATE TABLE catalog_cart_items (
	id BIGINT NOT NULL PRIMARY KEY,
	cart_id BIGINT NOT NULL,
	variant_id BIGINT NOT NULL,
	quantity INT NOT NULL,
	unit_price DECIMAL(10,2) NOT NULL,
	INDEX idx_catalog_cart_items_cart (cart_id),
	INDEX idx_catalog_cart_items_variant (variant_id)
);

INSERT INTO catalog_categories (id, name)
SELECT n, CONCAT('Category ', n)
FROM (
	SELECT ones.i + 1 AS n
	FROM (SELECT 0 i UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) ones
) numbers;

INSERT INTO catalog_products (id, category_id, sku, name, price, active, created_at)
SELECT
	n,
	((n - 1) MOD 5) + 1,
	CONCAT('PROD-', LPAD(n, 4, '0')),
	CONCAT('Sample Product ', n),
	ROUND(12.50 + (n * 1.37), 2),
	IF(n MOD 7 = 0, 0, 1),
	DATE_ADD('2026-01-01 09:00:00', INTERVAL n DAY)
FROM (
	SELECT ones.i + tens.i * 10 + hundreds.i * 100 + 1 AS n
	FROM (SELECT 0 i UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) ones
	CROSS JOIN (SELECT 0 i UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) tens
	CROSS JOIN (SELECT 0 i UNION ALL SELECT 1) hundreds
) numbers
WHERE n <= 120;

INSERT INTO catalog_variants (id, product_id, sku, color, size, inventory)
SELECT
	(product_id * 10) + size_id,
	product_id,
	CONCAT('PROD-', LPAD(product_id, 4, '0'), '-', size_code),
	CASE product_id MOD 4
		WHEN 0 THEN 'Black'
		WHEN 1 THEN 'Blue'
		WHEN 2 THEN 'Green'
		ELSE 'Red'
	END,
	size_code,
	(product_id * size_id) MOD 47
FROM (
	SELECT id AS product_id FROM catalog_products
) products
CROSS JOIN (
	SELECT 1 AS size_id, 'S' AS size_code UNION ALL
	SELECT 2, 'M' UNION ALL
	SELECT 3, 'L'
) sizes;

INSERT INTO catalog_customers (id, email, display_name, created_at)
SELECT
	n,
	CONCAT('customer', n, '@example.test'),
	CONCAT('Customer ', n),
	DATE_ADD('2026-02-01 10:00:00', INTERVAL n HOUR)
FROM (
	SELECT ones.i + tens.i * 10 + 1 AS n
	FROM (SELECT 0 i UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) ones
	CROSS JOIN (SELECT 0 i UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) tens
) numbers
WHERE n <= 40;

INSERT INTO catalog_carts (id, customer_id, status, created_at)
SELECT
	n,
	((n - 1) MOD 40) + 1,
	IF(n MOD 5 = 0, 'checked_out', 'open'),
	DATE_ADD('2026-03-01 11:00:00', INTERVAL n HOUR)
FROM (
	SELECT ones.i + tens.i * 10 + 1 AS n
	FROM (SELECT 0 i UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) ones
	CROSS JOIN (SELECT 0 i UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7) tens
) numbers
WHERE n <= 80;

INSERT INTO catalog_cart_items (id, cart_id, variant_id, quantity, unit_price)
SELECT
	(cart_id * 10) + line_id,
	cart_id,
	((((cart_id + line_id) - 1) MOD 120) + 1) * 10 + (((line_id - 1) MOD 3) + 1),
	line_id,
	ROUND(14.00 + (line_id * 2.25) + (cart_id MOD 9), 2)
FROM (
	SELECT id AS cart_id FROM catalog_carts
) carts
CROSS JOIN (
	SELECT 1 AS line_id UNION ALL
	SELECT 2 UNION ALL
	SELECT 3
) item_lines;
