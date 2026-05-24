-- name: HealthDBPing :one
-- 数据库健康检查（不查表，只 ping 连接）
SELECT 1::INTEGER AS ok;
