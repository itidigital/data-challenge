WITH calendar as (SELECT date_trunc('day', dd)::date "day"
FROM generate_series('1900-01-01'::date, (date_trunc('year', CURRENT_DATE) + '1 year'::interval - '1 day'::interval + '24 hours'::interval - '1 second'::interval), '1 day'::interval) dd)

SELECT cst.customer_id,trx.account_id, cst."name",trx.dt, trx.transaction_type, ROUND(avg(trx.amount),2) mean_value FROM (
SELECT account_id, dt, amount, 'pix_send' transaction_type
FROM pix_send
UNION ALL
SELECT account_id, dt, amount, 'pix_received' transaction_type
FROM pix_received
UNION ALL
SELECT account_id, due_date, amount, 'bankslip' transaction_type
FROM bankslip
UNION ALL
SELECT account_id_source, dt, amount, 'p2p_tef' transaction_type
FROM p2p_tef
UNION ALL
SELECT account_id_destination::int, dt, amount, 'p2p_tef' transaction_type
FROM p2p_tef) trx
JOIN account acc on acc.account_id = trx.account_id
JOIN customer cst on cst.customer_id = acc.customer_id
JOIN calendar cld on cld."day" = trx.dt
GROUP BY cst.customer_id, cst."name", trx.account_id, trx.dt, trx.transaction_type
ORDER BY 1