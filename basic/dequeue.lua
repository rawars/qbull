-- dequeue.lua
local queue_key = KEYS[1]          -- Clave de la cola (por ejemplo, "queue:audio transcoding:jobs")

-- Obtener el ID del siguiente trabajo
local job_id = redis.call('LPOP', queue_key)

if job_id then
    -- Actualizar el estado del trabajo a "processing"
    redis.call('HSET', 'queue:job:' .. job_id, 'status', 'processing')
    -- Obtener los datos del trabajo
    local job_data = redis.call('HGET', 'queue:job:' .. job_id, 'data')
    return {job_id, job_data}
else
    return nil
end
