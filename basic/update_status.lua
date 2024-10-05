-- update_status.lua
local job_id = ARGV[1]             -- ID del trabajo
local new_status = ARGV[2]         -- Nuevo estado del trabajo

-- Verificar si el trabajo existe
local exists = redis.call('EXISTS', 'queue:job:' .. job_id)
if exists == 1 then
    -- Actualizar el estado del trabajo
    redis.call('HSET', 'queue:job:' .. job_id, 'status', new_status)
    return 'OK'
else
    return 'Job not found'
end
