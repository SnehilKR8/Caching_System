
# TOP_CUSTOMERS_QUERY = """
# SELECT s.suppkey, s.name, SUM(l.quantity) AS total_quantity
# FROM supplier s
# JOIN lineitem l ON s.suppkey = l.suppkey
# GROUP BY s.suppkey, s.name
# ORDER BY total_quantity DESC
# LIMIT 10;
# """



# TOP_CUSTOMERS_QUERY = """
# SELECT orders.custkey, COUNT(orders.orderkey) AS order_count
# FROM orders
# GROUP BY orders.custkey
# ORDER BY order_count DESC
# LIMIT 10;
# """




TOP_CUSTOMERS_QUERY = """
SELECT supplier.name, SUM(lineitem.extendedprice * (1 - lineitem.discount)) AS revenue
FROM supplier
JOIN lineitem ON supplier.suppkey = lineitem.suppkey
GROUP BY supplier.name
ORDER BY revenue DESC
LIMIT 10;
"""




# TOP_CUSTOMERS_QUERY = """
# SELECT customer.mktsegment, COUNT(DISTINCT orders.orderkey) AS total_orders
# FROM customer
# JOIN orders ON customer.custkey = orders.custkey
# GROUP BY customer.mktsegment
# ORDER BY total_orders DESC;

# """





# TOP_CUSTOMERS_QUERY = """
# SELECT region.name AS region_name, COUNT(DISTINCT part.partkey) AS total_parts
# FROM region
# JOIN nation ON region.regionkey = nation.regionkey
# JOIN supplier ON supplier.nationkey = nation.nationkey
# JOIN partsupp ON partsupp.suppkey = supplier.suppkey
# JOIN part ON part.partkey = partsupp.partkey
# GROUP BY region.name
# ORDER BY total_parts DESC;
# """