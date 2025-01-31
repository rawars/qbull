// index.js
import { Queue } from './index.js';
import pino from 'pino';
import pinoPretty from 'pino-pretty';

const logger = pino(
    {
        level: 'info'
    },
    pinoPretty({ translateTime: 'SYS:standard' })
);

const queue = new Queue(
    {
        host: 'localhost',
        port: 6379
    },
    {
        inactivityTimeout: 5000,
        maxActiveConsumers: 5,
        poolSize: 5
    }
);

const subscription = queue.getJobEvents().subscribe({
    next: (event) => logger.info({ event }, 'Evento'),
    error: (err) => logger.error(err),
    complete: () => logger.info('Finalizado')
});

// Función de procesamiento
queue.process('WHATSAPP', async function (job, done) {
    try {
        logger.info(`Procesando trabajo ${job.id} del grupo: ${job.groupName}`);
        await new Promise((resolve) => setTimeout(resolve, job.data.sleep));
        job.progress(46);
        await new Promise((resolve) => setTimeout(resolve, 2000));
        job.progress(100);
        done(null, { result: 'Mensaje enviado' });
    } catch (error) {
        done(error);
    }
});

// Ejemplo de uso mixto:
// 1) Agregar trabajos individuales

queue.add('WHATSAPP', '573205108541', { message: 'Hola, usuario 1', sleep: 5000 });
queue.add('WHATSAPP', '573205108541', { message: 'Hola, usuario 1 - segundo mensaje', sleep: 0 });
queue.add('WHATSAPP', '573205108542', { message: 'Hola, usuario 2', sleep: 5000 });

// Detener el procesamiento tras 30s
setTimeout(() => {
    queue.stop();
    subscription.unsubscribe();
    logger.info('Procesamiento detenido.');
}, 30000);
