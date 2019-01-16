SELECT l_receiptdate
FROM lineitem, orders
WHERE	l_orderkey = o_orderkey
