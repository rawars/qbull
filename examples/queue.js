import { Queue } from '../index.js'; // o tu import local

// Instancia de la cola
export default new Queue(
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