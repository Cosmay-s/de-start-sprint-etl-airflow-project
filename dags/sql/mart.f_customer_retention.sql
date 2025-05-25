DROP TABLE IF EXISTS mart.f_customer_retention;

CREATE TABLE mart.f_customer_retention (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    new_customers_count INTEGER NOT NULL,
    returning_customers_count INTEGER NOT NULL,
    refunded_customer_count INTEGER NOT NULL,
    period_name VARCHAR(20) NOT NULL,
    period_id VARCHAR(20) NOT NULL,
    item_id INTEGER NOT NULL,
    new_customers_revenue NUMERIC(12,2) NOT NULL,
    returning_customers_revenue NUMERIC(12,2) NOT NULL,
    customers_refunded NUMERIC(12,0) NOT NULL
);

WITH base_data AS (
    SELECT
        fs.customer_id,
        fs.item_id,
        fs.status,
        fs.payment_amount,
        DATE_TRUNC('week', dc.date_actual) AS week_start,
        EXTRACT(WEEK FROM dc.date_actual) AS period_id
    FROM mart.f_sales fs
    JOIN mart.d_calendar dc ON fs.date_id = dc.date_id
    WHERE dc.date_actual <= '{{ ds }}'::DATE
),
week_data AS (
    SELECT *
    FROM base_data
    WHERE week_start = DATE_TRUNC('week', '{{ ds }}'::DATE)
),
agg_by_customer AS (
    SELECT
        customer_id,
        item_id,
        COUNT(*) AS order_count,
        SUM(CASE WHEN status = 'shipped' THEN payment_amount ELSE 0 END) AS shipped_revenue,
        SUM(CASE WHEN status = 'refunded' THEN -payment_amount ELSE 0 END) AS refunded_revenue,
        MAX(CASE WHEN status = 'refunded' THEN 1 ELSE 0 END) AS was_refunded,
        MAX(period_id) AS period_id
    FROM week_data
    GROUP BY customer_id, item_id
)
INSERT INTO mart.f_customer_retention (
    new_customers_count,
    returning_customers_count,
    refunded_customer_count,
    period_name,
    period_id,
    item_id,
    new_customers_revenue,
    returning_customers_revenue,
    customers_refunded
)
SELECT
    COUNT(*) FILTER (WHERE order_count = 1) AS new_customers_count,
    COUNT(*) FILTER (WHERE order_count > 1) AS returning_customers_count,
    COUNT(*) FILTER (WHERE was_refunded = 1) AS refunded_customer_count,
    'weekly' AS period_name,
    period_id::TEXT,
    item_id,
    SUM(CASE WHEN order_count = 1 THEN shipped_revenue ELSE 0 END) AS new_customers_revenue,
    SUM(CASE WHEN order_count > 1 THEN shipped_revenue ELSE 0 END) AS returning_customers_revenue,
    SUM(refunded_revenue) AS customers_refunded
FROM agg_by_customer
GROUP BY period_id, item_id;
