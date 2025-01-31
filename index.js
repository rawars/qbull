// Solo necesitas quitar las referencias a this.enqueueShaPromise, this.dequeueShaPromise, etc.,
// y usar las propiedades this.enqueueSha, this.dequeueSha, this.updateStatusSha, etc.
// Además de asegurarte de hacer await this.scriptsLoaded antes de usarlas, así:

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

        this.publisher = new Redis(redisConfig);
        this.subscriber = new Redis(redisConfig);

        this.subscribeToNewJobs();

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

        this.batchStatus = new Map();

        // Cargar scripts y asignarlos a propiedades
        this.scriptsLoaded = (async () => {
            const client = await this.pool.acquire();
            try {
                this.enqueueSha = await client.script('LOAD', this.enqueueScript);
                this.dequeueSha = await client.script('LOAD', this.dequeueScript);
                this.updateStatusSha = await client.script('LOAD', this.updateStatusScript);
                this.getStatusSha = await client.script('LOAD', this.getStatusScript);
            } finally {
                this.pool.release(client);
            }
        })();

        // Verificar trabajos pendientes al iniciar
        this.checkPendingJobs();

    }

    async checkPendingJobs() {
        await this.scriptsLoaded;
        const client = await this.pool.acquire();
        try {
            const queues = await client.keys('queue:*:groups');
            for (const queueKey of queues) {
                const queueName = queueKey.split(':')[1];
                const groups = await client.smembers(queueKey);
                for (const groupName of groups) {
                    const groupKey = `queue:${queueName}:group:${groupName}`;
                    const jobCount = await client.llen(groupKey);
                    if (jobCount > 0) {
                        this.startGroupConsumer(queueName, groupName);
                    }
                }
            }
        } finally {
            this.pool.release(client);
        }
    }

    subscribeToNewJobs() {
        this.subscriber.subscribe('queue:new_job', (err) => {
            if (err) console.error('Error al suscribirse:', err);
        });
        this.subscriber.on('message', (channel, message) => {
            if (channel === 'queue:new_job') {
                const { queueName, groupName } = JSON.parse(message);
                if (this.processFunctions.has(queueName)) {
                    this.startGroupConsumer(queueName, groupName);
                }
            }
        });
    }

    getJobEvents() {
        return this.jobEvents$.asObservable();
    }

    async process(queueName, processFunction) {
        await this.scriptsLoaded;
        this.processFunctions.set(queueName, processFunction);

        const client = await this.pool.acquire();

        try {
            const groups = await client.smembers(`queue:${queueName}:groups`);
            groups.forEach(groupName => {
                if (groupName.startsWith(`queue:${queueName}:group:`)) {
                    groupName = groupName.split(`queue:${queueName}:group:`)[1];
                }
                this.startGroupConsumer(queueName, groupName);
            });
        } finally {
            this.pool.release(client);
        }
    }

    async add(queueName, groupName, jobData) {
        // Asegúrate de esperar a que se carguen los scripts
        await this.scriptsLoaded;
        const client = await this.pool.acquire();
        try {
            const queueKey = `queue:${queueName}:groups`;
            const groupKey = `queue:${queueName}:group:${groupName}`;
            // Usa la propiedad this.enqueueSha en lugar de this.enqueueShaPromise
            const jobId = await client.evalsha(
                this.enqueueSha,
                2,
                queueKey,
                groupKey,
                JSON.stringify(jobData),
                groupName
            );
            await this.publisher.publish('queue:new_job', JSON.stringify({ queueName, groupName }));
            return jobId;
        } finally {
            this.pool.release(client);
        }
    }

    async addBatch(queueName, groupName, jobsData) {
        const batchId = uuidv4();
        this.batchStatus.set(batchId, { total: jobsData.length, completed: 0 });

        const promises = jobsData.map(async (data) => {
            return this.add(queueName, groupName, { ...data, batchId });
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
        await this.scriptsLoaded;
        while (true) {
            let redisClient;
            try {
                redisClient = await this.pool.acquire();
                const result = await redisClient.evalsha(this.dequeueSha, 1, groupKey);
                if (result) {
                    this.resetGroupConsumerTimer(queueName, groupName);
                    const [jobId, jobDataRaw, groupNameFromJob] = result;
                    const jobDataParsed = jobDataRaw ? JSON.parse(jobDataRaw) : null;
                    const job = {
                        id: jobId,
                        data: jobDataParsed,
                        groupName: groupNameFromJob ? groupNameFromJob.toString() : undefined,
                        progress: (value) => this.updateProgress(jobId, value)
                    };
                    await this.processJob(queueName, job, this.processFunctions.get(queueName));
                } else {
                    const shouldStop = await this.checkGroupConsumerInactivity(queueName, groupName);
                    if (shouldStop) {
                        this.stopGroupConsumer(queueName, groupName);
                        break;
                    } else {
                        await new Promise((r) => setTimeout(r, 1000));
                    }
                }
            } catch (error) {
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

    checkBatchCompletion(batchId) {
        if (!batchId) return;
        const batchInfo = this.batchStatus.get(batchId);
        if (!batchInfo) return;

        batchInfo.completed += 1;
        if (batchInfo.completed >= batchInfo.total) {
            this.jobEvents$.next({ type: 'batchCompleted', batchId });
            this.batchStatus.delete(batchId);
        } else {
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
        // Espera a que se carguen los scripts y usa this.updateStatusSha
        await this.scriptsLoaded;
        let client;
        try {
            client = await this.pool.acquire();
            await client.evalsha(this.updateStatusSha, 0, jobId, newStatus);
            this.emit(newStatus, jobId);
        } finally {
            this.pool.release(client);
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