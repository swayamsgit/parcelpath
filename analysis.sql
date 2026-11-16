-- Delivered-only denominator; exclude missing deliveries from the rate.
SELECT city, COUNT(*) AS orders,
 ROUND(SUM(net_paise)/100.0,2) AS net_merchandise_inr,
 SUM(delivered) AS delivered_orders,
 ROUND(100.0*(SUM(delivered)-SUM(late))/NULLIF(SUM(delivered),0),2) AS on_time_pct
FROM mart_order
GROUP BY city
ORDER BY net_merchandise_inr DESC;
