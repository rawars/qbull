// queue.js
import Redis from 'ioredis';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { EventEmitter } from 'events';

// https://chatgpt.com/c/670085b4-b5f4-800f-aef7-c40a9b8affc3

// Monitorización y Escalabilidad: Considera implementar métricas y registros adicionales para monitorizar el rendimiento y ajustar los parámetros de configuración en consecuencia.

// Si en el futuro necesitas manejar prioridades entre los grupos en espera, puedes implementar una estructura de datos más avanzada (como una cola de prioridad) para pendingGroupConsumers.

// TODO: Esto al final a ver q: Ajuste Dinámico: Puedes ajustar maxActiveConsumers y inactivityTimeout según las necesidades y la carga de tu sistema.

export class Queue extends EventEmitter {
    constructor(redisConfig, options = {}) {
        super();
        this.redis = new Redis(redisConfig);

        this.inactivityTimeout = options.inactivityTimeout || 10000; // Tiempo de inactividad
        this.maxActiveConsumers = options.maxActiveConsumers || Infinity; // Límite de consumidores

        this.activeGroupConsumers = new Map(); // Consumidores activos
        this.pendingGroupConsumers = []; // Consumidores pendientes

        // Cargar los scripts Lua
        const __filename = fileURLToPath(import.meta.url);
        const __dirname = path.dirname(__filename);

        this.enqueueScript = fs.readFileSync(path.join(__dirname, 'enqueue.lua'), 'utf8');
        this.dequeueScript = fs.readFileSync(path.join(__dirname, 'dequeue.lua'), 'utf8');
        this.updateStatusScript = fs.readFileSync(path.join(__dirname, 'update_status.lua'), 'utf8');
        this.getStatusScript = fs.readFileSync(path.join(__dirname, 'get_status.lua'), 'utf8');

        // Registrar los scripts en Redis y obtener sus SHA
        this.enqueueShaPromise = this.redis.script('LOAD', this.enqueueScript);
        this.dequeueShaPromise = this.redis.script('LOAD', this.dequeueScript);
        this.updateStatusShaPromise = this.redis.script('LOAD', this.updateStatusScript);
        this.getStatusShaPromise = this.redis.script('LOAD', this.getStatusScript);

        // Mapas para almacenar las funciones de procesamiento y consumidores activos
        this.processFunctions = new Map(); // clave: queueName, valor: processFunction
    }

    process(queueName, processFunction) {
        this.processFunctions.set(queueName, processFunction);
    }

    async add(queueName, groupName, jobData) {
        const sha = await this.enqueueShaPromise;
        const queueKey = `queue:${queueName}:groups`;
        const groupKey = `queue:${queueName}:group:${groupName}`;

        console.log('Agregando trabajo con groupName:', groupName);

        const jobId = await this.redis.evalsha(
            sha,
            2,
            queueKey,
            groupKey,
            JSON.stringify(jobData),
            groupName
        );
        console.log(`Trabajo encolado con ID: ${jobId} en cola: ${queueName}, grupo: ${groupName}`);

        // Iniciar el consumidor para el grupo si no está activo
        this.startGroupConsumer(queueName, groupName);

        return jobId;
    }

    startGroupConsumer(queueName, groupName) {
        const groupKey = `queue:${queueName}:group:${groupName}`;
        const consumerKey = `${queueName}:${groupName}`;

        if (this.activeGroupConsumers.has(consumerKey) || this.isConsumerPending(consumerKey)) {
            // El consumidor ya está activo o pendiente
            return;
        }

        if (this.activeGroupConsumers.size >= this.maxActiveConsumers) {
            // Añadir a la cola de espera
            this.pendingGroupConsumers.push({ queueName, groupName, groupKey });
            console.log(`Consumidor para el grupo ${groupName} en cola ${queueName} está en espera.`);
        } else {
            // Iniciar el consumidor inmediatamente
            const worker = this.groupWorker(queueName, groupName, groupKey);
            const timer = null;

            this.activeGroupConsumers.set(consumerKey, { worker, timer });
            console.log(`Consumidor para el grupo ${groupName} en cola ${queueName} iniciado.`);
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

        if (!processFunction) {
            console.error(`No hay función de procesamiento para la cola ${queueName}`);
            return;
        }

        const consumerKey = `${queueName}:${groupName}`;
        const redisClient = this.redis.duplicate();

        while (true) {
            try {
                const result = await redisClient.evalsha(dequeueSha, 1, groupKey);

                if (result) {
                    console.log('Resultado desde dequeue:', result);

                    this.resetGroupConsumerTimer(queueName, groupName);

                    const [jobId, jobDataRaw, groupNameFromJob] = result;

                    const jobDataString = jobDataRaw ? jobDataRaw.toString() : null;
                    const jobDataParsed = jobDataString ? JSON.parse(jobDataString) : null;

                    const groupNameStr = groupNameFromJob ? groupNameFromJob.toString() : undefined;

                    console.log('groupNameStr:');
                    console.log(groupNameStr);

                    const job = {
                        id: jobId,
                        data: jobDataParsed,
                        groupName: groupNameStr,
                        progress: (value) => this.updateProgress(jobId, value),
                    };

                    await this.processJob(queueName, job, processFunction);
                } else {
                    const shouldStop = await this.checkGroupConsumerInactivity(queueName, groupName);
                    if (shouldStop) {
                        this.stopGroupConsumer(queueName, groupName);
                        redisClient.disconnect();
                        break;
                    } else {
                        // Esperar un momento antes de volver a intentar
                        await new Promise((resolve) => setTimeout(resolve, 1000));
                    }
                }
            } catch (error) {
                console.error(
                    `Error en el consumidor del grupo ${groupName} en la cola ${queueName}:`,
                    error
                );
                // Esperar un momento antes de continuar en caso de error
                await new Promise((resolve) => setTimeout(resolve, 1000));
            }
        }
    }

    stopGroupConsumer(queueName, groupName) {
        const consumerKey = `${queueName}:${groupName}`;
        const consumerInfo = this.activeGroupConsumers.get(consumerKey);

        if (consumerInfo) {
            clearTimeout(consumerInfo.timer);
            this.activeGroupConsumers.delete(consumerKey);
            console.log(`Consumidor del grupo ${groupName} en cola ${queueName} detenido.`);
            // Procesar consumidores pendientes si los hay
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
            console.log(`Consumidor para el grupo ${groupName} en cola ${queueName} iniciado desde espera.`);
        }
    }

    async processJob(queueName, job, processFunction) {
        console.log('En processJob, job.groupName:', job.groupName);
        const done = async (err, result) => {
            if (err) {
                console.error(`Error en el trabajo ${job.id}:`, err);
                await this.updateJobStatus(job.id, 'failed');
            } else {
                console.log(`Trabajo ${job.id} completado con resultado:`, result);
                await this.updateJobStatus(job.id, 'completed');
            }
        };

        try {
            await processFunction(job, done);
        } catch (error) {
            console.error(`Excepción no manejada en el trabajo ${job.id}:`, error);
            await this.updateJobStatus(job.id, 'failed');
        }
    }

    async updateProgress(jobId, value) {
        await this.redis.hset(`queue:job:${jobId}`, 'progress', value);
        console.log(`Progreso del trabajo ${jobId}: ${value}%`);
    }

    async updateJobStatus(jobId, newStatus) {
        const sha = await this.updateStatusShaPromise;
        await this.redis.evalsha(sha, 0, jobId, newStatus);
        console.log(`Estado del trabajo ${jobId} actualizado a: ${newStatus}`);
        this.emit(newStatus, jobId);
    }

    resetGroupConsumerTimer(queueName, groupName) {
        const consumerKey = `${queueName}:${groupName}`;
        const consumerInfo = this.activeGroupConsumers.get(consumerKey);

        if (consumerInfo) {
            if (consumerInfo.timer) {
                clearTimeout(consumerInfo.timer);
            }

            consumerInfo.timer = setTimeout(() => {
                consumerInfo.shouldStop = true;
            }, this.inactivityTimeout);
        }
    }

    async checkGroupConsumerInactivity(queueName, groupName) {
        const consumerKey = `${queueName}:${groupName}`;
        const consumerInfo = this.activeGroupConsumers.get(consumerKey);

        if (consumerInfo && consumerInfo.shouldStop) {
            return true;
        }

        return false;
    }

    stop() {
        this.redis.disconnect();
        for (const [key, { worker, timer }] of this.activeGroupConsumers.entries()) {
            clearTimeout(timer);
            // No necesitamos desconectar los clientes aquí, ya que se desconectarán al salir de los bucles
        }
        this.activeGroupConsumers.clear();
        this.pendingGroupConsumers = [];
    }
}
