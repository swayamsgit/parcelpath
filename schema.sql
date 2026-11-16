PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS dim_customer(customer_id TEXT PRIMARY KEY, city TEXT NOT NULL, segment TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS dim_product(product_id TEXT PRIMARY KEY, category TEXT NOT NULL, name TEXT NOT NULL);
-- Grain: one product line per order in this deliberately bounded demo.
CREATE TABLE IF NOT EXISTS fact_order(
 order_id TEXT PRIMARY KEY, customer_id TEXT NOT NULL REFERENCES dim_customer,
 product_id TEXT NOT NULL REFERENCES dim_product, order_date TEXT NOT NULL,
 quantity INTEGER NOT NULL CHECK(quantity>0), gross_paise INTEGER NOT NULL CHECK(gross_paise>=0),
 discount_paise INTEGER NOT NULL CHECK(discount_paise>=0 AND discount_paise<=gross_paise),
 updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS fact_shipment(
 order_id TEXT PRIMARY KEY REFERENCES fact_order, promised_date TEXT NOT NULL,
 delivered_date TEXT, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS load_manifest(
 source TEXT NOT NULL, sha256 TEXT NOT NULL, accepted INTEGER NOT NULL,
 rejected INTEGER NOT NULL, loaded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 PRIMARY KEY(source,sha256));
CREATE TABLE IF NOT EXISTS quarantine(
 source TEXT NOT NULL, sha256 TEXT NOT NULL, line INTEGER NOT NULL,
 reason TEXT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(source,sha256,line));
CREATE INDEX IF NOT EXISTS ix_order_date ON fact_order(order_date);
CREATE INDEX IF NOT EXISTS ix_order_customer ON fact_order(customer_id);
CREATE INDEX IF NOT EXISTS ix_order_product ON fact_order(product_id);
CREATE VIEW IF NOT EXISTS mart_order AS
 SELECT o.order_id,o.order_date,c.city,c.segment,p.category,o.quantity,
 o.gross_paise-o.discount_paise AS net_paise,
 CASE WHEN s.delivered_date IS NOT NULL THEN 1 ELSE 0 END AS delivered,
 CASE WHEN s.delivered_date IS NOT NULL AND s.delivered_date>s.promised_date THEN 1 ELSE 0 END AS late,
 CASE WHEN s.delivered_date IS NOT NULL THEN julianday(s.delivered_date)-julianday(o.order_date) END AS delivery_days
 FROM fact_order o JOIN dim_customer c USING(customer_id)
 JOIN dim_product p USING(product_id) LEFT JOIN fact_shipment s USING(order_id);
