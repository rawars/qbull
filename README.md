# QBull

Biblioteca sencilla para manejar colas de procesamiento por grupos (y soporte opcional de “batches”) usando [Redis](https://redis.io/).

## Instalación

```bash
npm install qbull
```
## Configuración

Importa la clase Queue desde tu archivo con la implementación (o directamente desde el paquete si ya lo publicaste). Luego, crea una instancia de la clase pasando la configuración de Redis y las opciones de la cola:

```javascript
import { Queue } from 'qbull'; // o desde tu archivo local

const queue = new Queue(
  {
    host: 'localhost',
    port: 6379,
    // password: 'mypassword'  // opcional si tu Redis lo requiere
  },
  {
    inactivityTimeout: 5000,   // Tiempo (ms) que un consumidor puede estar inactivo antes de detenerse
    maxActiveConsumers: 5,     // Máximo número de consumidores que pueden estar activos a la vez
    poolSize: 5               // Número de conexiones de Redis en el pool
  }
);
```
#### Parámetros relevantes

- **inactivityTimeout**: Tiempo (en milisegundos) que se esperará si no hay trabajos en cola para un grupo. Una vez cumplido el tiempo sin recibir más trabajos, se libera el consumidor de ese grupo.
  - Valor típico: `5000` (5 segundos)
  - Ajusta según tu escenario

- **maxActiveConsumers**: Cantidad máxima de grupos que pueden tener un consumidor activo simultáneamente.
  - Por ejemplo, `1` para procesar un grupo a la vez; `5` para permitir 5 grupos en paralelo.

- **poolSize**: Tamaño del pool de conexiones Redis.
  - Entre `3` y `10` suele ser razonable.

## Definir la función de procesamiento

Registra la función de procesamiento con:

```javascript
queue.process('NOMBRE_COLA', async (job, done) => {
  // Tu lógica aquí
});
```
El callback recibe:
- **job**: ```{ id, data, groupName, progress }```
  - ```job.progress(%)```: Actualiza el progreso del trabajo.

- **done(error, resultado)**: .
  - Si se llama con ```done(null, resultado)```, el trabajo se marca como completed.
  - Si se llama con ```done(error)```, se marca como failed.

#### Ejemplo:
```javascript
queue.process('WHATSAPP', async function (job, done) {
  try {
    console.log(`Procesando trabajo ${job.id} del grupo: ${job.groupName}`);
    await new Promise(resolve => setTimeout(resolve, job.data.sleep || 1000));
    job.progress(50);
    await new Promise(resolve => setTimeout(resolve, 2000));
    job.progress(100);
    done(null, { result: 'Mensaje enviado' });
  } catch (error) {
    done(error);
  }
});
```

## Agregar trabajos individuales:
```javascript
await queue.add('WHATSAPP', 'grupo1', { message: 'Hola!', sleep: 2000 });
```
- **Par1**: nombre de la cola (```WHATSAPP```).
- **Par2**: nombre del grupo (```grupo1```).
- **Par3**: datos del trabajo.

## Observabilidad con RxJS:
Suscríbete a los eventos para monitorear:

```javascript
const subscription = queue.getJobEvents().subscribe({
  next: (event) => console.log('Evento:', event),
  error: (err) => console.error('Error:', err),
  complete: () => console.log('Finalizado')
});
```
Se emiten eventos como:

- **progress**: ```{ type: 'progress', jobId, progress }```
- **completed**: ```{ type: 'completed', jobId }```
- **failed**: ```{ type: 'failed', jobId, error }```
- **batchProgress**: ```{ type: 'batchProgress', batchId, completed, total }```
- **batchCompleted**: ```{ type: 'batchCompleted', batchId }```

Para dejar de escuchar, usa ```subscription.unsubscribe()```.

## Trabajos por lote (Batches):
Se pueden agrupar varios trabajos con un mismo ```batchId```.
Cuando todos los trabajos de ese lote terminan (completan o fallan), se emite el evento **batchCompleted**.

```javascript
const { batchId, jobIds } = await queue.addBatch('WHATSAPP', 'miGrupo', [
  { message: 'Batch msg 1', sleep: 1000 },
  { message: 'Batch msg 2', sleep: 3000 },
  { message: 'Batch msg 3', sleep: 2000 }
]);
console.log('Batch creado:', batchId, jobIds);
```

## Detener la cola:
```javascript
await queue.stop();
subscription.unsubscribe();
```

# Contribución
- Haz un fork del repositorio.
- Crea una rama con tu feature (git checkout -b my-feature).
- Realiza tus cambios y haz commit.
- Envía un Pull Request.
- Agradecemos tus aportes para mejorar QBull.

¡Eso es todo! Esperamos que esta biblioteca te ayude a manejar tus colas de trabajos agrupados de forma sencilla y eficiente. Cualquier duda o sugerencia será bienvenida.