"""
E-commerce traffic generator.

Simulates a live online shop by continuously inserting, updating and deleting
rows in the source Postgres database, so that CDC (Debezium) has a steady
stream of changes to capture.
"""

import logging
import os
import random
import secrets
import signal
import sys
import time
import unicodedata
from decimal import ROUND_HALF_UP, Decimal

import psycopg
from faker import Faker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("generator")

fake = Faker("pl_PL")

INTERVAL_SECONDS = float(os.environ.get("GENERATOR_INTERVAL_SECONDS", "2"))

CONNECTION_PARAMS = {
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "port": int(os.environ.get("POSTGRES_PORT", "5432")),
    "dbname": os.environ["POSTGRES_DB"],
    "user": os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"],
}

# Order lifecycle: current status -> possible next statuses
# (a value listed several times has a higher probability)
NEXT_STATUS = {
    "new": ["paid", "paid", "paid", "cancelled"],
    "paid": ["shipped"],
    "shipped": ["delivered"],
}

running = True


def handle_stop(signum, frame):
    global running
    log.info("Stop signal received, finishing current action...")
    running = False


signal.signal(signal.SIGTERM, handle_stop)
signal.signal(signal.SIGINT, handle_stop)


def to_ascii(text):
    """Make a name safe for an e-mail address, e.g. 'Michał' -> 'michal'."""
    text = text.replace("\u0142", "l").replace("\u0141", "L")
    normalized = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    return normalized.lower().replace(" ", "").replace("-", "")


def create_customer(cur):
    first_name = fake.first_name()
    last_name = fake.last_name()
    email = f"{to_ascii(first_name)}.{to_ascii(last_name)}.{secrets.token_hex(3)}@example.com"
    cur.execute(
        """
        INSERT INTO ecommerce.customers (email, first_name, last_name, city)
        VALUES (%s, %s, %s, %s)
        RETURNING customer_id
        """,
        (email, first_name, last_name, fake.city()),
    )
    return cur.fetchone()[0]


# ---------------------------------------------------------------------------
# Actions - each returns a log message, or None when there is nothing to do
# ---------------------------------------------------------------------------

def new_customer(cur):
    customer_id = create_customer(cur)
    return f"INSERT customer {customer_id}"


def new_order(cur):
    cur.execute("SELECT customer_id FROM ecommerce.customers ORDER BY random() LIMIT 1")
    row = cur.fetchone()
    customer_id = row[0] if row else create_customer(cur)

    cur.execute(
        "INSERT INTO ecommerce.orders (customer_id) VALUES (%s) RETURNING order_id",
        (customer_id,),
    )
    order_id = cur.fetchone()[0]

    cur.execute(
        """
        SELECT product_id, price
        FROM ecommerce.products
        WHERE is_active
        ORDER BY random()
        LIMIT %s
        """,
        (random.randint(1, 4),),
    )
    products = cur.fetchall()
    for product_id, price in products:
        cur.execute(
            """
            INSERT INTO ecommerce.order_items (order_id, product_id, quantity, unit_price)
            VALUES (%s, %s, %s, %s)
            """,
            (order_id, product_id, random.randint(1, 3), price),
        )
    return f"INSERT order {order_id} (customer {customer_id}, {len(products)} item(s))"


def advance_order(cur):
    cur.execute(
        """
        SELECT order_id, status
        FROM ecommerce.orders
        WHERE status IN ('new', 'paid', 'shipped')
        ORDER BY random()
        LIMIT 1
        """
    )
    row = cur.fetchone()
    if row is None:
        return None
    order_id, status = row
    new_status = random.choice(NEXT_STATUS[status])
    cur.execute(
        "UPDATE ecommerce.orders SET status = %s WHERE order_id = %s",
        (new_status, order_id),
    )
    return f"UPDATE order {order_id}: {status} -> {new_status}"


def return_order(cur):
    cur.execute(
        """
        SELECT order_id
        FROM ecommerce.orders
        WHERE status = 'delivered'
        ORDER BY random()
        LIMIT 1
        """
    )
    row = cur.fetchone()
    if row is None:
        return None
    cur.execute(
        "UPDATE ecommerce.orders SET status = 'returned' WHERE order_id = %s",
        (row[0],),
    )
    return f"UPDATE order {row[0]}: delivered -> returned"


def remove_order_item(cur):
    # Customer removes one item from an unpaid order that has more than one item
    cur.execute(
        """
        SELECT oi.order_item_id, oi.order_id
        FROM ecommerce.order_items oi
        JOIN ecommerce.orders o ON o.order_id = oi.order_id
        WHERE o.status = 'new'
          AND oi.order_id IN (
              SELECT order_id
              FROM ecommerce.order_items
              GROUP BY order_id
              HAVING count(*) > 1
          )
        ORDER BY random()
        LIMIT 1
        """
    )
    row = cur.fetchone()
    if row is None:
        return None
    order_item_id, order_id = row
    cur.execute(
        "DELETE FROM ecommerce.order_items WHERE order_item_id = %s",
        (order_item_id,),
    )
    return f"DELETE item {order_item_id} from order {order_id}"


def change_price(cur):
    cur.execute("SELECT product_id, sku, price FROM ecommerce.products ORDER BY random() LIMIT 1")
    row = cur.fetchone()
    if row is None:
        return None
    product_id, sku, old_price = row
    factor = Decimal(str(round(random.uniform(0.85, 1.15), 4)))
    new_price = (old_price * factor).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    new_price = max(new_price, Decimal("1.00"))
    cur.execute(
        "UPDATE ecommerce.products SET price = %s WHERE product_id = %s",
        (new_price, product_id),
    )
    return f"UPDATE product {sku}: price {old_price} -> {new_price}"


def customer_moves(cur):
    cur.execute("SELECT customer_id, city FROM ecommerce.customers ORDER BY random() LIMIT 1")
    row = cur.fetchone()
    if row is None:
        return None
    customer_id, old_city = row
    new_city = fake.city()
    cur.execute(
        "UPDATE ecommerce.customers SET city = %s WHERE customer_id = %s",
        (new_city, customer_id),
    )
    return f"UPDATE customer {customer_id}: city {old_city} -> {new_city}"


# Action -> weight (relative probability, sums to 100 for readability)
ACTIONS = [
    (new_customer, 15),
    (new_order, 30),
    (advance_order, 30),
    (return_order, 7),
    (remove_order_item, 5),
    (change_price, 8),
    (customer_moves, 5),
]


def connect():
    for attempt in range(1, 31):
        try:
            conn = psycopg.connect(**CONNECTION_PARAMS, autocommit=True)
            log.info("Connected to Postgres at %s:%s", CONNECTION_PARAMS["host"], CONNECTION_PARAMS["port"])
            return conn
        except psycopg.OperationalError as exc:
            log.warning("Postgres not ready (attempt %s/30): %s", attempt, exc)
            time.sleep(2)
    raise SystemExit("Could not connect to Postgres, giving up")


def main():
    functions = [action for action, _ in ACTIONS]
    weights = [weight for _, weight in ACTIONS]

    log.info("Generator started, interval = %.1f s", INTERVAL_SECONDS)
    conn = connect()

    while running:
        action = random.choices(functions, weights=weights, k=1)[0]
        try:
            # One action = one transaction (e.g. an order and its items commit together)
            with conn.transaction():
                with conn.cursor() as cur:
                    message = action(cur)
                    if message is None:
                        message = new_order(cur)
            log.info(message)
        except psycopg.OperationalError:
            log.exception("Connection problem, reconnecting...")
            conn = connect()
        except psycopg.Error:
            log.exception("Action %s failed, skipping", action.__name__)
        time.sleep(INTERVAL_SECONDS)

    conn.close()
    log.info("Generator stopped")


if __name__ == "__main__":
    main()