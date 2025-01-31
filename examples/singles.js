import pino from 'pino';
import pinoPretty from 'pino-pretty';

import QUEUE from './queue.js';

// Agregar trabajos
(async () => {

    console.log("run")

    // Logger
    const logger = pino(
        { level: 'info' },
        pinoPretty({ translateTime: 'SYS:standard' })
    );

    // Observabilidad
    /*
    const subscription = QUEUE.getJobEvents().subscribe({
        next: (event) => logger.info({ event }, 'Evento'),
        error: (err) => logger.error(err),
        complete: () => logger.info('Finalizado')
    });
    */

    console.log("Iniciando consumidor en singles.js");

    // Definir proceso
    QUEUE.process('WHATSAPP', async (job, done) => {
        try {
            logger.info(`Procesando trabajo ${job.id} (grupo: ${job.groupName})`);
            console.log(job.data)
            
            await new Promise(resolve => setTimeout(resolve, job.data.sleep || 1000));
            job.progress(50);
            await new Promise(resolve => setTimeout(resolve, 2000));
            job.progress(100);
            done(null, { result: 'Mensaje enviado' });
        } catch (error) {
            done(error);
        }
    });

    console.log("Iniciando consumidor en singles.js");

    // Esperar un tiempo y luego detener
    /*
    setTimeout(() => {
        QUEUE.stop();
        // subscription.unsubscribe();
        logger.info('Procesamiento detenido.');
    }, 20000);
    */

    setInterval(() => { }, 1000);

})();
