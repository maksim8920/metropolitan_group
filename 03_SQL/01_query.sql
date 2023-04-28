SELECT
	DISTINCT ARRAY_LENGTH(phone) AS phone_counts,
	ROUND(((count(*) OVER (PARTITION BY ARRAY_LENGTH(phone)))*1.0 / (SELECT count(*) FROM test_map))*100, 2) as phone_counts_percent
FROM 
	raw_data.crm_contacts