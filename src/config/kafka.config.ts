// Configuración centralizada de Kafka (equivalente a spring.kafka en application.yml + KafkaConfig del demo).
// Este servicio crea y publica el tópico "catalogo", y consume "reservas" (que crea hotel-ms-reservas).
import { NOMBRE_SERVICIO } from './servicio';

const CATORCE_DIAS_MS = 14 * 24 * 60 * 60 * 1000;

export interface DefinicionTopico {
	nombre: string;
	particiones: number;
	replicas: number;
	configs: Record<string, string>;
}

function lista(variable: string | undefined): string[] {
	return (variable ?? '').split(',').map((valor) => valor.trim()).filter(Boolean);
}

function entero(variable: string | undefined, porDefecto: number): number {
	const valor = Number(variable);
	return Number.isInteger(valor) && valor > 0 ? valor : porDefecto;
}

export const kafkaConfig = {
	/** bootstrap-servers. Si está vacío, Kafka queda desactivado (tests y desarrollo sin brokers). */
	brokers: lista(process.env.KAFKA_BROKERS),
	clientId: process.env.KAFKA_CLIENT_ID ?? NOMBRE_SERVICIO,
	reconexionMs: entero(process.env.KAFKA_RECONEXION_MS, 10000),

	productor: {
		/** acks=all: el evento queda en todas las réplicas en sincronía (mínimo 2) antes de confirmarse. */
		acks: -1,
		idempotente: true,
		reintentos: 5
	},

	consumidor: {
		/** Igual que auto-offset-reset: earliest. Solo aplica cuando el grupo todavía no tiene offset guardado. */
		fromBeginning: true,
		sessionTimeoutMs: 30000,
		heartbeatIntervalMs: 3000
	},

	/** FixedBackOff(1000, 2) de la guía 3.3.3: 3 intentos en total, 1 s entre cada uno. */
	reintentos: {
		intentos: entero(process.env.KAFKA_REINTENTOS_INTENTOS, 3),
		esperaMs: entero(process.env.KAFKA_REINTENTOS_ESPERA_MS, 1000)
	},

	/** El DLT de cada tópico se llama igual con ".DLT" (como pedidos.DLT). */
	sufijoDlt: '.DLT',

	topicos: {
		/**
		 * Estado actual de cada habitación (precio, unidades, ocupación). Key = idHabitacion.
		 * compact: Kafka guarda el último evento de cada habitación y descarta los anteriores, así el
		 * tópico es siempre "el catálogo vigente" (en "reservas" se usa delete porque interesa el historial).
		 */
		catalogo: {
			nombre: process.env.KAFKA_TOPICO_CATALOGO ?? 'catalogo',
			particiones: 3,
			replicas: 3,
			configs: {
				'cleanup.policy': 'compact',
				'min.insync.replicas': '2',
				// Segmentos de 1 hora: la compactación puede actuar sobre segmentos cerrados, y así se nota en la demo.
				'segment.ms': '3600000'
			}
		},
		catalogoDlt: {
			nombre: `${process.env.KAFKA_TOPICO_CATALOGO ?? 'catalogo'}.DLT`,
			particiones: 3,
			replicas: 3,
			configs: {
				'cleanup.policy': 'delete',
				'retention.ms': String(CATORCE_DIAS_MS),
				'min.insync.replicas': '2'
			}
		}
	} satisfies Record<string, DefinicionTopico>,

	/** Tópico que se consume (lo crea hotel-ms-reservas). */
	topicoReservas: process.env.KAFKA_TOPICO_RESERVAS ?? 'reservas',

	grupos: {
		/** Reacción en tiempo real: actualiza la disponibilidad de la habitación al llegar cada reserva. */
		disponibilidad: process.env.KAFKA_GRUPO ?? 'disponibilidad',
		disponibilidadDlt: `${process.env.KAFKA_GRUPO ?? 'disponibilidad'}-dlt`
	},

	/** Un miembro por partición de "reservas": las reservas de una habitación siempre las procesa el mismo. */
	concurrencia: entero(process.env.KAFKA_CONCURRENCIA, 3)
} as const;

export function topicoDlt(topico: string): string {
	return `${topico}${kafkaConfig.sufijoDlt}`;
}
