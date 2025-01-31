import QUEUE from './queue.js';

// Agregar trabajos
(async () => {
    await QUEUE.add('WHATSAPP', 'grupoA', { message: 'Hola1', sleep: 2000 });
    await QUEUE.add('WHATSAPP', 'grupoA', { message: 'Hola2', sleep: 1000 });
    await QUEUE.add('WHATSAPP', 'grupoB', { message: 'Hola3', sleep: 3000 });
    console.log("TRABAJOS AGREGADOS");
})();
