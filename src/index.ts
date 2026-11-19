import 'dotenv/config';
import { crearApp } from './app';
import { crearPoolBaseDatos } from './config/database';
import { HABITACIONES_INICIALES, HOTELES_INICIALES } from './config/datosIniciales';
import { kafkaConfig } from './config/kafka.config';
import { DisponibilidadListener } from './consumers/DisponibilidadListener';
import { DltListener } from './consumers/DltListener';
import { log } from './logs/logger';
import { ContenedorKafka } from './messaging/ContenedorKafka';
import { listenersKafkaRegistrados } from './messaging/KafkaListener';
import { crearProductorEventosCatalogo } from './messaging/productorEventosCatalogo';
import { CatalogoRepository, InMemoryCatalogoRepository, MySqlCatalogoRepository } from './repositories/catalogoRepository';
import { CatalogoService } from './services/catalogoService';

const puerto = Number(process.env.PORT ?? 8088);
const pool = crearPoolBaseDatos();
const repositorio: CatalogoRepository = pool ? new MySqlCatalogoRepository(pool) : new InMemoryCatalogoRepository();
if (!pool) {
	log('warn', 'CATALOGO_EN_MEMORIA', { detalle: 'MYSQL_HOST no definido' });
}
const productor = crearProductorEventosCatalogo();
const servicio = new CatalogoService(repositorio, productor);

// Cada instancia registra sus métodos @KafkaListener; el contenedor los suscribe al conectar.
new DisponibilidadListener(servicio);
new DltListener();

const contenedor = kafkaConfig.brokers.length > 0
	? new ContenedorKafka([...kafkaConfig.brokers], listenersKafkaRegistrados)
	: undefined;
if (!contenedor) {
	log('warn', 'KAFKA_DESACTIVADO', { detalle: 'KAFKA_BROKERS no definido' });
}

async function cargarDatosIniciales(): Promise<void> {
	try {
		await repositorio.inicializar(HOTELES_INICIALES, HABITACIONES_INICIALES);
		log('info', 'CATALOGO_LISTO', { hoteles: HOTELES_INICIALES.length, habitaciones: HABITACIONES_INICIALES.length });
	} catch (error) {
		log('error', 'CATALOGO_NO_INICIALIZADO', { motivo: error instanceof Error ? error.message : String(error), reintentoEnMs: 10000 });
		setTimeout(() => void cargarDatosIniciales(), 10000);
	}
}

const servidor = crearApp({ servicio, kafkaConectado: () => contenedor?.estaConectado ?? false }).listen(puerto, () => {
	log('info', 'SERVICIO_INICIADO', { puerto });
	void cargarDatosIniciales();
	// El productor crea "catalogo" y "catalogo.DLT"; el contenedor suscribe el grupo "disponibilidad" a "reservas".
	void productor.iniciar();
	void contenedor?.iniciar();
});

for (const senal of ['SIGINT', 'SIGTERM'] as const) {
	process.once(senal, () => {
		servidor.close();
		void Promise.all([contenedor?.detener(), productor.cerrar(), pool?.end()]).finally(() => process.exit(0));
	});
}
