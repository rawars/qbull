// index.js
import { Queue } from './queue.js';



const queue = new Queue(
    {
        host: '192.168.49.2',
        port: 31679,
        // password: 'foobared',
    },
    {
        inactivityTimeout: 5000,       // Tiempo de inactividad de 20 segundos
        maxActiveConsumers: 1          // Límite de 2 consumidores activos simultáneamente
    }
);

// Optimización: Si planeas escalar el sistema, considera ajustar el tiempo de inactividad o implementar mecanismos para limitar el número de consumidores activos simultáneamente.

// Definir la función de procesamiento para la cola 'WHATSAPP'
queue.process('WHATSAPP', async function (job, done) {
    try {
        console.log(`Procesando trabajo ${job.id} del grupo: ${job.groupName}`);
        // Simular procesamiento
        await new Promise((resolve) => setTimeout(resolve, 2000));
        // Reportar progreso
        job.progress(46); // Porcentaje de ejemplo
        // Simular más procesamiento
        await new Promise((resolve) => setTimeout(resolve, 2000));
        // Reportar progreso final
        job.progress(100);
        // Finalizar el trabajo
        done(null, { result: 'Mensaje enviado' });
    } catch (error) {
        done(error);
    }
});

// Agregar trabajos a la cola 'WHATSAPP' con diferentes grupos
queue.add('WHATSAPP', '573205108541', { message: 'Hola, usuario 1' });
queue.add('WHATSAPP', '573205108541', { message: 'Hola, usuario 1 - segundo mensaje' });
queue.add('WHATSAPP', '573205108542', { message: 'Hola, usuario 2' });
queue.add('WHATSAPP', '573205108543', { message: 'Hola, usuario 3' });
queue.add('WHATSAPP', '573205108542', { message: 'Hola, usuario 2 - segundo mensaje' });

// Opción para detener el procesamiento después de cierto tiempo (por ejemplo, 30 segundos)
setTimeout(() => {
    queue.stop();
    console.log('Procesamiento detenido.');
}, 30000);
