import { Queue } from 'qbull'; // o tu import local
import pino from 'pino';
import pinoPretty from 'pino-pretty';

// Logger
const logger = pino(
    { level: 'info' },
    pinoPretty({ translateTime: 'SYS:standard' })
);

// Instancia de la cola
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

// Observabilidad
const subscription = queue.getJobEvents().subscribe({
    next: (event) => logger.info({ event }, 'Evento'),
    error: (err) => logger.error(err),
    complete: () => logger.info('Finalizado')
});

// Definir proceso
queue.process('WHATSAPP', async (job, done) => {
    try {
        logger.info(`Procesando trabajo ${job.id} (grupo: ${job.groupName})`);
        await new Promise(resolve => setTimeout(resolve, job.data.sleep || 1000));
        job.progress(50);
        await new Promise(resolve => setTimeout(resolve, 2000));
        job.progress(100);
        done(null, { result: 'Mensaje enviado' });
    } catch (error) {
        done(error);
    }
});

// Agregar trabajos
(async () => {
    await queue.add('WHATSAPP', 'grupoA', { message: 'Hola1', sleep: 2000 });
    await queue.add('WHATSAPP', 'grupoA', { message: 'Hola2', sleep: 1000 });
    await queue.add('WHATSAPP', 'grupoB', { message: 'Hola3', sleep: 3000 });
    // Esperar un tiempo y luego detener
    setTimeout(() => {
        queue.stop();
        subscription.unsubscribe();
        logger.info('Procesamiento detenido.');
    }, 20000);
})();
