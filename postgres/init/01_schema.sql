-- =========================================================
-- E-commerce source schema (OLTP) - designed for CDC
-- =========================================================

CREATE SCHEMA IF NOT EXISTS ecommerce;

-- ---------------------------------------------------------
-- Customers
-- ---------------------------------------------------------
CREATE TABLE ecommerce.customers (
    customer_id  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email        TEXT        NOT NULL UNIQUE,
    first_name   TEXT        NOT NULL,
    last_name    TEXT        NOT NULL,
    city         TEXT        NOT NULL,
    country      TEXT        NOT NULL DEFAULT 'PL',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------
-- Products
-- ---------------------------------------------------------
CREATE TABLE ecommerce.products (
    product_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sku          TEXT          NOT NULL UNIQUE,
    name         TEXT          NOT NULL,
    category     TEXT          NOT NULL,
    price        NUMERIC(10,2) NOT NULL CHECK (price > 0),
    is_active    BOOLEAN       NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------
-- Orders
-- ---------------------------------------------------------
CREATE TABLE ecommerce.orders (
    order_id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id  BIGINT      NOT NULL REFERENCES ecommerce.customers (customer_id),
    status       TEXT        NOT NULL DEFAULT 'new'
                 CHECK (status IN ('new', 'paid', 'shipped', 'delivered', 'cancelled', 'returned')),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_orders_customer_id ON ecommerce.orders (customer_id);

-- ---------------------------------------------------------
-- Order items
-- ---------------------------------------------------------
CREATE TABLE ecommerce.order_items (
    order_item_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id      BIGINT        NOT NULL REFERENCES ecommerce.orders (order_id) ON DELETE CASCADE,
    product_id    BIGINT        NOT NULL REFERENCES ecommerce.products (product_id),
    quantity      INTEGER       NOT NULL CHECK (quantity > 0),
    unit_price    NUMERIC(10,2) NOT NULL CHECK (unit_price > 0),
    created_at    TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX idx_order_items_order_id   ON ecommerce.order_items (order_id);
CREATE INDEX idx_order_items_product_id ON ecommerce.order_items (product_id);

-- ---------------------------------------------------------
-- Auto-update updated_at on every UPDATE
-- ---------------------------------------------------------
CREATE OR REPLACE FUNCTION ecommerce.set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_customers_updated_at   BEFORE UPDATE ON ecommerce.customers   FOR EACH ROW EXECUTE FUNCTION ecommerce.set_updated_at();
CREATE TRIGGER trg_products_updated_at    BEFORE UPDATE ON ecommerce.products    FOR EACH ROW EXECUTE FUNCTION ecommerce.set_updated_at();
CREATE TRIGGER trg_orders_updated_at      BEFORE UPDATE ON ecommerce.orders      FOR EACH ROW EXECUTE FUNCTION ecommerce.set_updated_at();
CREATE TRIGGER trg_order_items_updated_at BEFORE UPDATE ON ecommerce.order_items FOR EACH ROW EXECUTE FUNCTION ecommerce.set_updated_at();

-- ---------------------------------------------------------
-- CDC: log full "before" image of every changed row
-- ---------------------------------------------------------
ALTER TABLE ecommerce.customers   REPLICA IDENTITY FULL;
ALTER TABLE ecommerce.products    REPLICA IDENTITY FULL;
ALTER TABLE ecommerce.orders      REPLICA IDENTITY FULL;
ALTER TABLE ecommerce.order_items REPLICA IDENTITY FULL;