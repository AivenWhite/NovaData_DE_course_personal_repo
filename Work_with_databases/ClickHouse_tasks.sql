-- Создание таблицы user_events
create table user_events(
	user_id UInt32,
	event_type String,
	points_spent UInt32,
	event_time DateTime
) ENGINE = MergeTree()
ORDER BY (event_time, user_id)
TTL event_time + INTERVAL 30 DAY;


-- Создание таблицы user_events_agg
create table user_events_agg(
	event_date Date,
	event_type String,
	uniq_users_state AggregateFunction(uniq, UInt32),
	points_spent_state AggregateFunction(sum, UInt32),
	action_count_state AggregateFunction(count, UInt8)
) ENGINE = AggregatingMergeTree()
ORDER BY (event_date, event_type)
TTL event_date + INTERVAL 180 DAY;


-- Создание materialized view 
CREATE MATERIALIZED VIEW events_agg_mv
TO user_events_agg
AS
SELECT 
	toDate(event_time) as event_date,
	event_type,
	uniqState(user_id) as uniq_users_state,
	sumState(points_spent) as points_spent_state,
	countState() as action_count_state 
FROM user_events
GROUP BY event_date, event_type



-- Добавление тестовых данных
INSERT INTO user_events VALUES
(1, 'login', 0, now() - INTERVAL 10 DAY),
(2, 'signup', 0, now() - INTERVAL 10 DAY),
(3, 'login', 0, now() - INTERVAL 10 DAY),
(1, 'login', 0, now() - INTERVAL 7 DAY),
(2, 'login', 0, now() - INTERVAL 7 DAY),
(3, 'purchase', 30, now() - INTERVAL 7 DAY),
(1, 'purchase', 50, now() - INTERVAL 5 DAY),
(2, 'logout', 0, now() - INTERVAL 5 DAY),
(4, 'login', 0, now() - INTERVAL 5 DAY),
(1, 'login', 0, now() - INTERVAL 3 DAY),
(3, 'purchase', 70, now() - INTERVAL 3 DAY),
(5, 'signup', 0, now() - INTERVAL 3 DAY),
(2, 'purchase', 20, now() - INTERVAL 1 DAY),
(4, 'logout', 0, now() - INTERVAL 1 DAY),
(5, 'login', 0, now() - INTERVAL 1 DAY),
(1, 'purchase', 25, now()),
(2, 'login', 0, now()),
(3, 'logout', 0, now()),
(6, 'signup', 0, now()),
(6, 'purchase', 100, now());


-- Рассчёт Retention
WITH first_events AS (
    -- Определение первого события для каждого пользователя
    SELECT
        user_id,
        MIN(event_time) AS first_event_time
    FROM user_events
    GROUP BY user_id
),
returned_users AS (
    -- Определение пользователей, которые вернулись в течение 7 дней после первого события
    SELECT DISTINCT
        e.user_id
    FROM user_events e
    JOIN first_events c ON e.user_id = c.user_id
    WHERE e.event_time > c.first_event_time
      AND e.event_time <= c.first_event_time + INTERVAL 7 DAY
)
SELECT
    COUNT(DISTINCT c.user_id) AS total_users_day_0,
    COUNT(DISTINCT r.user_id) AS returned_in_7_days,
    ROUND((COUNT(DISTINCT r.user_id) * 100.0 / NULLIF(COUNT(DISTINCT c.user_id), 0)), 2) AS retention_7d_percent
FROM first_events c
LEFT JOIN returned_users r ON c.user_id = r.user_id;


-- Пример запроса с группировками по быстрой аналитике по дням
select 
	event_date, 
	event_type,
	uniqMerge(uniq_users_state) as unique_users,
	sumMerge(points_spent_state) as total_points_spent,
	countMerge(action_count_state) as total_actions
from user_events_agg
group by event_date, event_type
order by event_date


