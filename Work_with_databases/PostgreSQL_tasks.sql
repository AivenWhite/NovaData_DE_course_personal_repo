-- Создание таблицы users
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT,
    role TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Создание таблицы users_audit
CREATE TABLE users_audit (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    changed_by TEXT,
    field_changed TEXT,
    old_value TEXT,
    new_value TEXT
);


-- Создание функции логирования изменений 
CREATE OR REPLACE FUNCTION log_user_changes()
RETURNS TRIGGER AS $$
BEGIN
    -- Проверка изменения поля name
    IF NEW.name IS DISTINCT FROM OLD.name THEN
        INSERT INTO users_audit (user_id, changed_by, field_changed, old_value, new_value)
        VALUES (OLD.id, current_user, 'name', OLD.name, NEW.name);
    END IF;

    -- Проверка изменения поля email
    IF NEW.email IS DISTINCT FROM OLD.email THEN
        INSERT INTO users_audit (user_id, changed_by, field_changed, old_value, new_value)
        VALUES (OLD.id, current_user, 'email', OLD.email, NEW.email);
    END IF;

    -- Проверка изменения поля role
    IF NEW.role IS DISTINCT FROM OLD.role THEN
        INSERT INTO users_audit (user_id, changed_by, field_changed, old_value, new_value)
        VALUES (OLD.id, current_user, 'role', OLD.role, NEW.role);
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Создание триггера на изменения в таблице users
CREATE TRIGGER user_change_trigger
BEFORE UPDATE ON users
FOR EACH ROW
EXECUTE FUNCTION log_user_changes();


-- Добавление данных в таблицу users
insert into users (name, email, role)
values 
('Ivan Ivanov', 'ivan@example.com', 'Junior DE'),
('Anna Petrova', 'anna@example.com', 'Junior DA');


-- Проверка добавления данных в таблицу users
select * from users;


-- Изменение данных в таблице users
update users u 
set role = 'Middle DA'
where name = 'Anna Petrova';


-- Проверка изменения данных в таблице users
select * from users;


-- Проверка cрабатывания триггера => добавления изменений в таблицу users_audit
select * from users_audit;


-- Подключение расширения pg_cron
CREATE EXTENSION IF NOT exists pg_cron;


-- Создание функции экспорта изменений в csv
CREATE OR REPLACE FUNCTION export_audit_to_csv()
RETURNS VOID AS $outer$
DECLARE
    path TEXT := '/tmp/users_audit_export_' || to_char(NOW(), 'YYYYMMDD_HH24MI') || '.csv';
    count_records INTEGER;
BEGIN
    -- Проверка количества записей, соответствующих критериям
    SELECT COUNT(*) INTO count_records
    FROM users_audit
    WHERE changed_at >= NOW() - interval '1 day';

    RAISE NOTICE 'Количество записей для экспорта: %', count_records;

    IF count_records > 0 THEN
        RAISE NOTICE 'Экспорт данных начат в файл: %', path;

        EXECUTE format(
            $inner$
            COPY (
                SELECT user_id, field_changed, old_value, new_value, changed_by, changed_at
                FROM users_audit
                WHERE changed_at >= NOW() - interval '1 day'
                ORDER BY changed_at
            ) TO '%s' WITH CSV HEADER
            $inner$, path
        );

        RAISE NOTICE 'Экспорт данных завершен.';
    ELSE
        RAISE NOTICE 'Нет данных для экспорта.';
    END IF;
END;
$outer$ LANGUAGE plpgsql;


--Проверка работы функции c последующей проверкой в контейнере
select export_audit_to_csv() 


--Добавление задачи по ежедневному экспорту изменений в планировщик
select cron.schedule(
	job_name := 'daily_audit_export',
	schedule := '0 3 * * *',
	command := $$select export_audit_to_csv();$$
);


-- Проверка добавления задачи в планировщик
select * from cron.job;








