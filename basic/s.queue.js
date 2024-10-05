// queue.js
import Redis from 'ioredis';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { EventEmitter } from 'events';

export class Queue extends EventEmitter {
    constructor(name, redisConfig) {
        super();
        this.name = name;
        this.redis = new Redis(redisConfig);

        // Cargar los scripts Lua
        const __filename = fileURLToPath(import.meta.url);
        const __dirname = path.dirname(__filename);

        this.enqueueScript = fs.readFileSync(path.join(__dirname, 'enqueue.lua'), 'utf8');
        this.dequeueScript = fs.readFileSync(path.join(__dirname, 'dequeue.lua'), 'utf8');
        this.getStatusScript = fs.readFileSync(path.join(__dirname, 'get_status.lua'), 'utf8');
        this.updateStatusScript = fs.readFileSync(path.join(__dirname, 'update_status.lua'), 'utf8');

        // Registrar los scripts en Redis y obtener sus SHA
        this.enqueueShaPromise = this.redis.script('LOAD', this.enqueueScript);
        this.dequeueShaPromise = this.redis.script('LOAD', this.dequeueScript);
        this.getStatusShaPromise = this.redis.script('LOAD', this.getStatusScript);
        this.updateStatusShaPromise = this.redis.script('LOAD', this.updateStatusScript);

        // Inicializar variables
        this.processFunction = null;
        this.concurrency = 1;
        this.stopped = false;
    }

    async add(jobData) {
        const sha = await this.enqueueShaPromise;
        const queueKey = `queue:${this.name}:jobs`;
        const jobId = await this.redis.evalsha(sha, 1, queueKey, JSON.stringify(jobData));
        console.log(`Trabajo encolado con ID: ${jobId} en cola: ${this.name}`);
        return jobId;
    }

    process(concurrency, fn) {
        if (typeof concurrency === 'function') {
            fn = concurrency;
            concurrency = 1;
        }

        this.processFunction = fn;
        this.concurrency = concurrency;
        this.startProcessing();
    }

    async startProcessing() {
        const workers = [];
        for (let i = 0; i < this.concurrency; i++) {
            workers.push(this.workerLoop(i));
        }
        await Promise.all(workers);
    }

    async workerLoop(workerId) {
        const sha = await this.dequeueShaPromise;
        const queueKey = `queue:${this.name}:jobs`;

        while (!this.stopped) {
            try {
                const result = await this.redis.evalsha(sha, 1, queueKey);
                if (result) {
                    const [jobId, jobData] = result;
                    const job = {
                        id: jobId,
                        data: JSON.parse(jobData),
                        progress: (value) => this.updateProgress(jobId, value),
                    };
                    await this.processJob(job, workerId);
                } else {
                    // Si no hay trabajos, esperar un momento antes de volver a intentar
                    await new Promise(resolve => setTimeout(resolve, 1000));
                }
            } catch (error) {
                console.error(`Error en el trabajador ${workerId}:`, error);
                // Esperar un momento antes de continuar en caso de error
                await new Promise(resolve => setTimeout(resolve, 1000));
            }
        }
    }

    async processJob(job, workerId) {
        const done = async (err, result) => {
            if (err) {
                console.error(`Error en el trabajo ${job.id} en trabajador ${workerId}:`, err);
                await this.updateJobStatus(job.id, 'failed');
            } else {
                console.log(`Trabajo ${job.id} completado por trabajador ${workerId} con resultado:`, result);
                await this.updateJobStatus(job.id, 'completed');
            }
        };

        try {
            await this.processFunction(job, done);
        } catch (error) {
            console.error(`Excepción no manejada en el trabajo ${job.id} en trabajador ${workerId}:`, error);
            await this.updateJobStatus(job.id, 'failed');
        }
    }

    async updateProgress(jobId, value) {
        // Almacenar el progreso en Redis
        await this.redis.hset(`queue:job:${jobId}`, 'progress', value);
        console.log(`Progreso del trabajo ${jobId}: ${value}%`);
    }

    async updateJobStatus(jobId, newStatus) {
        const sha = await this.updateStatusShaPromise;
        await this.redis.evalsha(sha, 0, jobId, newStatus);
        console.log(`Estado del trabajo ${jobId} actualizado a: ${newStatus}`);

        // Emitir evento según el nuevo estado
        this.emit(newStatus, jobId);
    }

    async getJobStatus(jobId) {
        const sha = await this.getStatusShaPromise;
        const result = await this.redis.evalsha(sha, 0, jobId);
        const [status, progress] = result;
        console.log(`Estado del trabajo ${jobId}: ${status}, Progreso: ${progress}%`);
        return { status, progress };
    }

    // Método para detener el procesamiento
    stop() {
        this.stopped = true;
        this.redis.disconnect();
    }
}
