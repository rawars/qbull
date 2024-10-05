-- get_status.lua
local job_id = ARGV[1]             -- ID del trabajo

-- Obtener el estado del trabajo
local status = redis.call('HGET', 'queue:job:' .. job_id, 'status')

return status
