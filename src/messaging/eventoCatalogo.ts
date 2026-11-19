import { randomUUID } from 'node:crypto';
import { Habitacion, Hotel, Ocupacion } from '../repositories/catalogoRepository';

export type TipoEventoCatalogo = 'HABITACION_CREADA' | 'HABITACION_ACTUALIZADA' | 'DISPONIBILIDAD_CAMBIADA';

/** Quién hizo el cambio: un administrador, o el propio sistema cuando reacciona a una reserva. */
export interface ActorCatalogoDto {
	tipo: 'INTERNO' | 'SISTEMA';
	id: string;
	nombre?: string;
	roles: string[];
}

/** Desde dónde: el panel admin o la reacción a un evento del tópico "reservas". */
export interface OrigenCatalogoDto {
	canal: 'PANEL_ADMIN' | 'KAFKA_RESERVAS';
	ip?: string;
	/** Evento de reserva que provocó el cambio (solo en DISPONIBILIDAD_CAMBIADA). */
	eventoReservaId?: string;
	codigoReserva?: string;
}

/**
 * Evento del tópico "catalogo" (compactado, key = idHabitacion). Lleva el estado COMPLETO de la habitación,
 * no solo lo que cambió: como Kafka conserva el último mensaje de cada key, leer el tópico desde el principio
 * entrega el catálogo vigente.
 */
export interface HabitacionEventoDto {
	eventoId: string;
	tipo: TipoEventoCatalogo;
	version: 1;
	ocurridoEn: string;
	actor: ActorCatalogoDto;
	origen: OrigenCatalogoDto;
	habitacion: {
		id: string;
		idHotel: string;
		nombreHotel: string;
		nombre: string;
		capacidad: number;
		precioPorNoche: number;
		unidades: number;
		activa: boolean;
		/** Reservas activas (pendientes o confirmadas) que todavía no terminan. */
		ocupaciones: { reservaId: string; desde: string; hasta: string }[];
	};
	/** Qué cambió, para la auditoría (por ejemplo, precio 45000 → 50000). */
	cambios?: { campo: string; antes: unknown; despues: unknown }[];
}

export function crearEventoHabitacion(
	tipo: TipoEventoCatalogo,
	habitacion: Habitacion,
	hotel: Hotel | undefined,
	ocupaciones: Ocupacion[],
	actor: ActorCatalogoDto,
	origen: OrigenCatalogoDto,
	cambios?: HabitacionEventoDto['cambios']
): HabitacionEventoDto {
	return {
		eventoId: randomUUID(),
		tipo,
		version: 1,
		ocurridoEn: new Date().toISOString(),
		actor,
		origen,
		habitacion: {
			id: habitacion.id,
			idHotel: habitacion.idHotel,
			nombreHotel: hotel?.nombre ?? habitacion.idHotel,
			nombre: habitacion.nombre,
			capacidad: habitacion.capacidad,
			precioPorNoche: habitacion.precioPorNoche,
			unidades: habitacion.unidades,
			activa: habitacion.activa,
			ocupaciones: ocupaciones.map((ocupacion) => ({
				reservaId: ocupacion.reservaId,
				desde: ocupacion.desde.toISOString(),
				hasta: ocupacion.hasta.toISOString()
			}))
		},
		...(cambios && cambios.length > 0 ? { cambios } : {})
	};
}
