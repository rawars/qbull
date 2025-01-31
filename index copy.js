// Ejemplo de soporte para batches en poolQueue.js
// Se define un método addBatch para agregar varios trabajos con un mismo batchId
// y emitir un evento cuando todos estén completados.

import { createPool } from 'generic-pool';
import Redis from 'ioredis';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { EventEmitter } from 'events';
import { Subject } from 'rxjs';
import { v4 as uuidv4 } from 'uuid';

export class Queue extends EventEmitter {
    constructor(redisConfig, options = {}) {
        super();

        this.jobEvents$ = new Subject();
        const poolSize = options.poolSize || 5;
        this.pool = createPool(
            {
                create: () => new Redis(redisConfig),
                destroy: (client) => client.quit()
            },
            {
                min: 1,
                max: poolSize
            }
        );

        this.inactivityTimeout = options.inactivityTimeout || 10000;
        this.maxActiveConsumers = options.maxActiveConsumers || Infinity;
        this.activeGroupConsumers = new Map();
        this.pendingGroupConsumers = [];
        this.processFunctions = new Map();

        const __filename = fileURLToPath(import.meta.url);
        const __dirname = path.dirname(__filename);

        this.enqueueScript = fs.readFileSync(path.join(__dirname, 'enqueue.lua'), 'utf8');
        this.dequeueScript = fs.readFileSync(path.join(__dirname, 'dequeue.lua'), 'utf8');
        this.updateStatusScript = fs.readFileSync(path.join(__dirname, 'update_status.lua'), 'utf8');
        this.getStatusScript = fs.readFileSync(path.join(__dirname, 'get_status.lua'), 'utf8');

        this.enqueueShaPromise = this.loadScript(this.enqueueScript);
        this.dequeueShaPromise = this.loadScript(this.dequeueScript);
        this.updateStatusShaPromise = this.loadScript(this.updateStatusScript);
        this.getStatusShaPromise = this.loadScript(this.getStatusScript);

        // Para rastrear el conteo de trabajos de cada batch
        this.batchStatus = new Map(); // batchId -> { total, completed }
    }

    getJobEvents() {
        return this.jobEvents$.asObservable();
    }

    async loadScript(script) {
        const client = await this.pool.acquire();
        try {
            const sha = await client.script('LOAD', script);
            return sha;
        } finally {
            this.pool.release(client);
        }
    }

    process(queueName, processFunction) {
        this.processFunctions.set(queueName, processFunction);
    }

    // Agregar un solo trabajo
    async add(queueName, groupName, jobData) {
        const sha = await this.enqueueShaPromise;
        const client = await this.pool.acquire();
        try {
            const queueKey = `queue:${queueName}:groups`;
            const groupKey = `queue:${queueName}:group:${groupName}`;
            const jobId = await client.evalsha(
                sha,
                2,
                queueKey,
                groupKey,
                JSON.stringify(jobData),
                groupName
            );
            this.startGroupConsumer(queueName, groupName);
            return jobId;
        } finally {
            this.pool.release(client);
        }
    }

    // Agregar varios trabajos como un batch
    async addBatch(queueName, groupName, jobsData) {
        const batchId = uuidv4(); // Generar un ID único para el batch
        // Registrar cuántos trabajos en total tiene el batch
        this.batchStatus.set(batchId, { total: jobsData.length, completed: 0 });

        // Agregar cada trabajo con info de batchId
        const promises = jobsData.map(async (data) => {
            const jobDataConBatch = {
                ...data,
                batchId
            };
            return this.add(queueName, groupName, jobDataConBatch);
        });
        return { batchId, jobIds: await Promise.all(promises) };
    }

    startGroupConsumer(queueName, groupName) {
        const groupKey = `queue:${queueName}:group:${groupName}`;
        const consumerKey = `${queueName}:${groupName}`;
        if (this.activeGroupConsumers.has(consumerKey) || this.isConsumerPending(consumerKey)) return;
        if (this.activeGroupConsumers.size >= this.maxActiveConsumers) {
            this.pendingGroupConsumers.push({ queueName, groupName, groupKey });
        } else {
            const worker = this.groupWorker(queueName, groupName, groupKey);
            const timer = null;
            this.activeGroupConsumers.set(consumerKey, { worker, timer });
        }
    }

    isConsumerPending(consumerKey) {
        return this.pendingGroupConsumers.some(
            (item) => `${item.queueName}:${item.groupName}` === consumerKey
        );
    }

    async groupWorker(queueName, groupName, groupKey) {
        const dequeueSha = await this.dequeueShaPromise;
        const processFunction = this.processFunctions.get(queueName);
        if (!processFunction) return;
        const consumerKey = `${queueName}:${groupName}`;

        while (true) {
            let redisClient;
            try {
                redisClient = await this.pool.acquire();
                const result = await redisClient.evalsha(dequeueSha, 1, groupKey);
                if (result) {
                    this.resetGroupConsumerTimer(queueName, groupName);
                    const [jobId, jobDataRaw, groupNameFromJob] = result;
                    const jobDataString = jobDataRaw ? jobDataRaw.toString() : null;
                    const jobDataParsed = jobDataString ? JSON.parse(jobDataString) : null;
                    const groupNameStr = groupNameFromJob ? groupNameFromJob.toString() : undefined;
                    const job = {
                        id: jobId,
                        data: jobDataParsed,
                        groupName: groupNameStr,
                        progress: (value) => this.updateProgress(jobId, value)
                    };
                    await this.processJob(queueName, job, processFunction);
                } else {
                    const shouldStop = await this.checkGroupConsumerInactivity(queueName, groupName);
                    if (shouldStop) {
                        this.stopGroupConsumer(queueName, groupName);
                        break;
                    } else {
                        await new Promise((r) => setTimeout(r, 1000));
                    }
                }
            } catch (_) {
                await new Promise((r) => setTimeout(r, 1000));
            } finally {
                if (redisClient) this.pool.release(redisClient);
            }
        }
    }

    stopGroupConsumer(queueName, groupName) {
        const consumerKey = `${queueName}:${groupName}`;
        const consumerInfo = this.activeGroupConsumers.get(consumerKey);
        if (consumerInfo) {
            clearTimeout(consumerInfo.timer);
            this.activeGroupConsumers.delete(consumerKey);
            this.processPendingConsumers();
        }
    }

    processPendingConsumers() {
        while (
            this.pendingGroupConsumers.length > 0 &&
            this.activeGroupConsumers.size < this.maxActiveConsumers
        ) {
            const { queueName, groupName, groupKey } = this.pendingGroupConsumers.shift();
            const consumerKey = `${queueName}:${groupName}`;
            const worker = this.groupWorker(queueName, groupName, groupKey);
            const timer = null;
            this.activeGroupConsumers.set(consumerKey, { worker, timer });
        }
    }

    async processJob(queueName, job, processFunction) {
        const done = async (err) => {
            if (err) {
                await this.updateJobStatus(job.id, 'failed');
                this.jobEvents$.next({ type: 'failed', jobId: job.id, error: err });
                this.checkBatchCompletion(job.data?.batchId);
            } else {
                await this.updateJobStatus(job.id, 'completed');
                this.jobEvents$.next({ type: 'completed', jobId: job.id });
                this.checkBatchCompletion(job.data?.batchId);
            }
        };
        try {
            await processFunction(job, done);
        } catch (error) {
            await this.updateJobStatus(job.id, 'failed');
            this.jobEvents$.next({ type: 'failed', jobId: job.id, error });
            this.checkBatchCompletion(job.data?.batchId);
        }
    }

    // Verificar si un batch se ha completado
    checkBatchCompletion(batchId) {
        if (!batchId) return;
        const batchInfo = this.batchStatus.get(batchId);
        if (!batchInfo) return;

        batchInfo.completed += 1;
        // Si el total de completados iguala el total de trabajos, emitimos batchCompleted
        if (batchInfo.completed >= batchInfo.total) {
            this.jobEvents$.next({
                type: 'batchCompleted',
                batchId
            });
            this.batchStatus.delete(batchId);
        } else {
            // Opcional: emitir un evento de “batchProgress” si quieres monitorear cuántos completados van
            this.jobEvents$.next({
                type: 'batchProgress',
                batchId,
                completed: batchInfo.completed,
                total: batchInfo.total
            });
        }
    }

    async updateProgress(jobId, value) {
        let client;
        try {
            client = await this.pool.acquire();
            await client.hset(`queue:job:${jobId}`, 'progress', value);
            this.jobEvents$.next({ type: 'progress', jobId, progress: value });
        } finally {
            if (client) this.pool.release(client);
        }
    }

    async updateJobStatus(jobId, newStatus) {
        const sha = await this.updateStatusShaPromise;
        let client;
        try {
            client = await this.pool.acquire();
            await client.evalsha(sha, 0, jobId, newStatus);
            this.emit(newStatus, jobId);
        } finally {
            if (client) this.pool.release(client);
        }
    }

    resetGroupConsumerTimer(queueName, groupName) {
        const consumerKey = `${queueName}:${groupName}`;
        const consumerInfo = this.activeGroupConsumers.get(consumerKey);
        if (consumerInfo) {
            if (consumerInfo.timer) clearTimeout(consumerInfo.timer);
            consumerInfo.timer = setTimeout(() => {
                consumerInfo.shouldStop = true;
            }, this.inactivityTimeout);
        }
    }

    async checkGroupConsumerInactivity(queueName, groupName) {
        const consumerKey = `${queueName}:${groupName}`;
        const consumerInfo = this.activeGroupConsumers.get(consumerKey);
        if (consumerInfo && consumerInfo.shouldStop) return true;
        return false;
    }

    async stop() {
        for (const [, { timer }] of this.activeGroupConsumers.entries()) {
            clearTimeout(timer);
        }
        this.activeGroupConsumers.clear();
        this.pendingGroupConsumers = [];
        await this.pool.drain();
        await this.pool.clear();
    }
}
