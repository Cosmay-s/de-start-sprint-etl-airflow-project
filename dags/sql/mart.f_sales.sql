-- Удаление старых записей
DELETE FROM mart.f_sales
WHERE date_id IN (
    SELECT dc.date_id
    FROM staging.user_order_log uol
    LEFT JOIN mart.d_calendar dc ON uol.date_time::DATE = dc.date_actual
    WHERE uol.date_time::DATE = '{{ ds }}'
);

-- Вставка новых данных, с учётом статуса заказа
INSERT INTO mart.f_sales (
    date_id,
    item_id,
    customer_id,
    city_id,
    quantity,
    payment_amount,
    status
)
SELECT
    dc.date_id,
    uol.item_id,
    uol.customer_id,
    uol.city_id,
    CASE 
        WHEN COALESCE(uol.status, 'shipped') = 'refunded' THEN -1 * uol.quantity
        ELSE uol.quantity
    END AS quantity,
    CASE 
        WHEN COALESCE(uol.status, 'shipped') = 'refunded' THEN -1 * uol.payment_amount
        ELSE uol.payment_amount
    END AS payment_amount,
    COALESCE(uol.status, 'shipped') AS status
FROM staging.user_order_log uol
LEFT JOIN mart.d_calendar dc ON uol.date_time::DATE = dc.date_actual
WHERE uol.date_time::DATE = '{{ ds }}';