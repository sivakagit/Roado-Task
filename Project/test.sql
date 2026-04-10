SET search_path TO nimbus;

SELECT ARRAY_AGG(DISTINCT s.customer_id) 
FROM subscriptions s
JOIN plans p ON p.plan_id = s.plan_id
WHERE p.plan_tier = 'free' AND s.status = 'active';


SET search_path TO nimbus;
SELECT STRING_AGG(DISTINCT s.customer_id::TEXT, ', ') 
FROM subscriptions s
JOIN plans p ON p.plan_id = s.plan_id
WHERE p.plan_tier = 'free' AND s.status = 'active';