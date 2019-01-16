SELECT SUM(n_regionkey), n_nationkey
FROM nation, region
WHERE n_regionkey = r_regionkey
GROUP BY n_nationkey