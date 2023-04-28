/*
1. Сначала удаляем данные за текущий месяц
2. pivot_deals - составляем сводную таблицу для типов лидов
Для каждого аккаунта считаем сколько разных типов лидов он привлек за один день
3. union_ads - объединяем данные из двух рекламных источников
4. Записываем данные в таблицу mart_data.marketing_account
*/


DELETE FROM mart_data.marketing_account
WHERE DATE_TRUNC(date, MONTH) = DATE_TRUNC('{{ dag.timezone.convert(execution_date).strftime("%Y-%m-%d") }}', MONTH);

WITH pivot_deals AS
    (SELECT *
    FROM 
        (SELECT 
            account_name, 
            date, 
            stage_of_lead, 
            COUNT(*) AS count
        FROM 
            crm_deals
        WHERE
            DATE_TRUNC(date, MONTH) = DATE_TRUNC('{{ dag.timezone.convert(execution_date).strftime("%Y-%m-%d") }}', MONTH)
        GROUP BY 
            account_name, 
            date, 
            stage_of_lead)
    PIVOT (
    count FOR stage_of_lead IN (
        SELECT DISTINCT stage_of_lead FROM crm_deals))),
union_ads AS
    (
    SELECT account_name, date, cost FROM google_ads
    WHERE DATE_TRUNC(date, MONTH) = DATE_TRUNC('{{ dag.timezone.convert(execution_date).strftime("%Y-%m-%d") }}', MONTH) 
    UNION
    SELECT account_name, date, cost FROM yandex_direct
    WHERE DATE_TRUNC(date, MONTH) = DATE_TRUNC('{{ dag.timezone.convert(execution_date).strftime("%Y-%m-%d") }}', MONTH)
    )
INSERT INTO mart_data.marketing_account
SELECT
    ua.date,
    ua.account_name,
    ua.cost,
    COALESCE(pd.stage_of_lead, 0) -- здесь были бы перечислены типы лидов
FROM
    pivot_deals AS pd
FULL JOIN
    union_ads as ua
        ON pd.date = ua.date
        AND pd.account_name = ua.account_name