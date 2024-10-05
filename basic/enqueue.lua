-- enqueue.lua
local queue_key = KEYS[1]          -- Clave de la cola (por ejemplo, "queue:audio transcoding:jobs")
local job_data = ARGV[1]           -- Datos del trabajo (por ejemplo, JSON)

-- Generar un ID único para el trabajo
local job_id = redis.call('INCR', 'queue:job_id')

-- Almacenar los datos del trabajo en un hash
redis.call('HSET', 'queue:job:' .. job_id, 'data', job_data, 'status', 'queued')

-- Añadir el ID del trabajo a la lista de la cola
redis.call('RPUSH', queue_key, job_id)

return tostring(job_id)
