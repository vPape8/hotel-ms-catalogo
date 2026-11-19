import { Kafka, logLevel, Partitioners, Producer } from 'kafkajs';
import { DefinicionTopico, kafkaConfig } from '../config/kafka.config';
import { log } from '../logs/logger';
import { HabitacionEventoDto } from './eventoCatalogo';

/** Lo único que conoce la lógica de negocio: no sabe de brokers, tópicos ni particiones. */
export interface ProductorEventosCatalogo {
	publicar(evento: HabitacionEventoDto): Promise<void>;
	iniciar(): Promise<void>;
	cerrar(): Promise<void>;
}

/** Se usa cuando KAFKA_BROKERS no está definida (tests, desarrollo sin Kafka): solo deja registro. */
export class ProductorKafkaDesactivado implements ProductorEventosCatalogo {
	async publicar(evento: HabitacionEventoDto): Promise<void> {
		log('warn', 'KAFKA_DESACTIVADO', { tipo: evento.tipo, eventoId: evento.eventoId });
	}

	async iniciar(): Promise<void> {}

	async cerrar(): Promise<void> {}
}

/** Serializer: key en texto (idHabitacion, la que usa la compactación) y valor en JSON del DTO. */
export function serializarEvento(evento: HabitacionEventoDto): { key: string; value: string; headers: Record<string, string> } {
	return {
		key: evento.habitacion.id,
		value: JSON.stringify(evento),
		headers: {
			tipo: evento.tipo,
			version: String(evento.version),
			eventoId: evento.eventoId,
			origen: kafkaConfig.clientId
		}
	};
}

export class ProductorKafka implements ProductorEventosCatalogo {
	private readonly kafka: Kafka;
	private productor?: Producer;
	private conectando?: Promise<Producer>;
	private detenido = false;

	constructor(brokers: string[]) {
		this.kafka = new Kafka({ clientId: kafkaConfig.clientId, brokers, logLevel: logLevel.WARN });
	}

	/** Al arrancar: crea los tópicos (como TopicBuilder en el demo) y conecta el productor. Si Kafka no está, reintenta. */
	async iniciar(): Promise<void> {
		this.detenido = false;
		try {
			await this.obtenerProductor();
		} catch (error) {
			log('error', 'KAFKA_CONEXION_FALLIDA', { motivo: error instanceof Error ? error.message : String(error), reintentoEnMs: kafkaConfig.reconexionMs });
			if (!this.detenido) {
				setTimeout(() => void this.iniciar(), kafkaConfig.reconexionMs);
			}
		}
	}

	async publicar(evento: HabitacionEventoDto): Promise<void> {
		const topico = kafkaConfig.topicos.catalogo.nombre;
		const mensaje = serializarEvento(evento);
		try {
			const productor = await this.obtenerProductor();
			const [metadata] = await productor.send({ topic: topico, acks: kafkaConfig.productor.acks, messages: [mensaje] });
			log('info', '[PUBLICA]', {
				topic: topico,
				key: mensaje.key,
				tipo: evento.tipo,
				eventoId: evento.eventoId,
				particion: metadata?.partition,
				offset: metadata?.baseOffset
			});
		} catch (error) {
			log('error', 'EVENTO_NO_PUBLICADO', {
				topic: topico,
				key: mensaje.key,
				tipo: evento.tipo,
				eventoId: evento.eventoId,
				motivo: error instanceof Error ? error.message : String(error)
			});
			throw error;
		}
	}

	async cerrar(): Promise<void> {
		this.detenido = true;
		const productor = this.productor;
		this.productor = undefined;
		await productor?.disconnect().catch(() => undefined);
	}

	private obtenerProductor(): Promise<Producer> {
		if (this.productor) {
			return Promise.resolve(this.productor);
		}
		this.conectando ??= this.conectar().finally(() => {
			this.conectando = undefined;
		});
		return this.conectando;
	}

	private async conectar(): Promise<Producer> {
		await this.crearTopicos(Object.values(kafkaConfig.topicos));
		const productor = this.kafka.producer({
			idempotent: kafkaConfig.productor.idempotente,
			maxInFlightRequests: 1,
			retry: { retries: kafkaConfig.productor.reintentos },
			createPartitioner: Partitioners.DefaultPartitioner
		});
		productor.on(productor.events.DISCONNECT, () => {
			this.productor = undefined;
		});
		await productor.connect();
		this.productor = productor;
		log('info', 'KAFKA_PRODUCTOR_CONECTADO', { topicos: Object.values(kafkaConfig.topicos).map((topico) => topico.nombre) });
		return productor;
	}

	/** Crea los tópicos que faltan con sus particiones, réplicas y configs. Si ya existen, no los cambia. */
	private async crearTopicos(topicos: DefinicionTopico[]): Promise<void> {
		const admin = this.kafka.admin();
		await admin.connect();
		try {
			const creados = await admin.createTopics({
				waitForLeaders: true,
				topics: topicos.map((topico) => ({
					topic: topico.nombre,
					numPartitions: topico.particiones,
					replicationFactor: topico.replicas,
					configEntries: Object.entries(topico.configs).map(([name, value]) => ({ name, value }))
				}))
			});
			if (creados) {
				log('info', 'KAFKA_TOPICOS_CREADOS', { topicos: topicos.map((topico) => topico.nombre) });
			}
		} finally {
			await admin.disconnect();
		}
	}
}

export function crearProductorEventosCatalogo(): ProductorEventosCatalogo {
	return kafkaConfig.brokers.length > 0 ? new ProductorKafka([...kafkaConfig.brokers]) : new ProductorKafkaDesactivado();
}
