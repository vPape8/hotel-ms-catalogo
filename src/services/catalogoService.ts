import { log } from '../logs/logger';
import { ActorCatalogoDto, crearEventoHabitacion, OrigenCatalogoDto, TipoEventoCatalogo } from '../messaging/eventoCatalogo';
import { ReservaEventoDto } from '../messaging/eventoReserva';
import { ProductorEventosCatalogo } from '../messaging/productorEventosCatalogo';
import { CambiosHabitacion, CatalogoRepository, Habitacion, HabitacionInicial, Hotel } from '../repositories/catalogoRepository';
import { ErrorValidacion } from './validarHabitacion';

export class RecursoNoEncontradoError extends Error {}
export class HabitacionDuplicadaError extends Error {}
export class CatalogoValidacionError extends Error {
	constructor(readonly detalles: ErrorValidacion[]) {
		super('Datos no válidos');
	}
}

export interface ContextoCambio {
	actor: ActorCatalogoDto;
	origen: OrigenCatalogoDto;
}

export interface HotelConHabitaciones extends Hotel {
	precioDesde: number | null;
	habitaciones: Habitacion[];
}

export interface Disponibilidad {
	idHabitacion: string;
	desde: string;
	hasta: string;
	unidades: number;
	ocupadas: number;
	disponibles: number;
	disponible: boolean;
}

export type ResultadoReaccion = 'ACTUALIZADA' | 'SIN_CAMBIOS' | 'HABITACION_DESCONOCIDA';

/** El catálogo reacciona solo: esta "persona" firma los cambios que provoca una reserva. */
const ACTOR_SISTEMA: ActorCatalogoDto = { tipo: 'SISTEMA', id: 'hotel-ms-catalogo', roles: [] };

function seCruzan(desdeA: Date, hastaA: Date, desdeB: Date, hastaB: Date): boolean {
	return desdeA < hastaB && desdeB < hastaA;
}

export class CatalogoService {
	constructor(
		private readonly repositorio: CatalogoRepository,
		private readonly productor: ProductorEventosCatalogo
	) {}

	// ---------- Lectura pública ----------

	async listarHoteles(): Promise<HotelConHabitaciones[]> {
		const [hoteles, habitaciones] = await Promise.all([this.repositorio.listarHoteles(), this.repositorio.listarHabitaciones()]);
		return hoteles.map((hotel) => this.conHabitaciones(hotel, habitaciones));
	}

	async obtenerHotel(id: string): Promise<HotelConHabitaciones> {
		const hotel = await this.repositorio.obtenerHotel(id);
		if (!hotel) throw new RecursoNoEncontradoError(`El hotel ${id} no existe`);
		return this.conHabitaciones(hotel, await this.repositorio.listarHabitaciones(id));
	}

	async obtenerHabitacion(id: string): Promise<Habitacion> {
		const habitacion = await this.repositorio.obtenerHabitacion(id);
		if (!habitacion) throw new RecursoNoEncontradoError(`La habitación ${id} no existe`);
		return habitacion;
	}

	/** Cuántas unidades de la habitación quedan libres entre dos fechas, según las reservas activas. */
	async disponibilidad(idHabitacion: string, desde: Date, hasta: Date): Promise<Disponibilidad> {
		const habitacion = await this.obtenerHabitacion(idHabitacion);
		const ocupaciones = await this.repositorio.ocupaciones(idHabitacion, desde);
		const ocupadas = ocupaciones.filter((ocupacion) => seCruzan(ocupacion.desde, ocupacion.hasta, desde, hasta)).length;
		const disponibles = habitacion.activa ? Math.max(0, habitacion.unidades - ocupadas) : 0;
		return {
			idHabitacion,
			desde: desde.toISOString(),
			hasta: hasta.toISOString(),
			unidades: habitacion.unidades,
			ocupadas,
			disponibles,
			disponible: disponibles > 0
		};
	}

	// ---------- Administración (rol ADMIN) ----------

	async crearHabitacion(datos: HabitacionInicial, contexto: ContextoCambio): Promise<Habitacion> {
		if (!(await this.repositorio.obtenerHotel(datos.idHotel))) {
			throw new CatalogoValidacionError([{ campo: 'idHotel', mensaje: `El hotel ${datos.idHotel} no existe` }]);
		}
		if (await this.repositorio.obtenerHabitacion(datos.id)) {
			throw new HabitacionDuplicadaError(`Ya existe la habitación ${datos.id}`);
		}
		const habitacion = await this.repositorio.crearHabitacion({ ...datos, activa: true, actualizadaEn: new Date() });
		void this.publicar('HABITACION_CREADA', habitacion, contexto).catch(() => undefined);
		return habitacion;
	}

	async actualizarHabitacion(id: string, cambios: CambiosHabitacion, contexto: ContextoCambio): Promise<Habitacion> {
		const antes = await this.obtenerHabitacion(id);
		const despues = await this.repositorio.actualizarHabitacion(id, cambios);
		if (!despues) throw new RecursoNoEncontradoError(`La habitación ${id} no existe`);
		const diferencias = (Object.keys(cambios) as (keyof CambiosHabitacion)[])
			.filter((campo) => antes[campo] !== despues[campo])
			.map((campo) => ({ campo, antes: antes[campo], despues: despues[campo] }));
		if (diferencias.length > 0) {
			void this.publicar('HABITACION_ACTUALIZADA', despues, contexto, diferencias).catch(() => undefined);
		}
		return despues;
	}

	// ---------- Reacción a los eventos de reservas ----------

	/**
	 * Reacción en tiempo real: una reserva PENDIENTE o CONFIRMADA ocupa una unidad entre sus fechas;
	 * una CANCELADA o COMPLETADA la libera. Si la ocupación cambió, publica DISPONIBILIDAD_CAMBIADA.
	 * Aquí sí se espera a Kafka: si la publicación falla, el consumidor reintenta y, si sigue fallando, va al DLT.
	 */
	async reaccionarAReserva(evento: ReservaEventoDto): Promise<ResultadoReaccion> {
		const { reserva } = evento;
		const habitacion = await this.repositorio.obtenerHabitacion(reserva.idHabitacion);
		if (!habitacion) {
			return 'HABITACION_DESCONOCIDA';
		}

		const cambio = reserva.estado === 'PENDIENTE' || reserva.estado === 'CONFIRMADA'
			? await this.repositorio.registrarOcupacion({
				reservaId: reserva.id,
				idHabitacion: reserva.idHabitacion,
				desde: new Date(reserva.fechaEntrada),
				hasta: new Date(reserva.fechaSalida),
				estado: reserva.estado
			})
			: await this.repositorio.liberarOcupacion(reserva.id);

		if (!cambio) {
			return 'SIN_CAMBIOS';
		}
		await this.publicar('DISPONIBILIDAD_CAMBIADA', habitacion, {
			actor: ACTOR_SISTEMA,
			origen: { canal: 'KAFKA_RESERVAS', eventoReservaId: evento.eventoId, codigoReserva: reserva.codigo }
		});
		return 'ACTUALIZADA';
	}

	// ---------- Internos ----------

	private conHabitaciones(hotel: Hotel, habitaciones: Habitacion[]): HotelConHabitaciones {
		const propias = habitaciones.filter((habitacion) => habitacion.idHotel === hotel.id && habitacion.activa);
		return {
			...hotel,
			precioDesde: propias.length > 0 ? Math.min(...propias.map((habitacion) => habitacion.precioPorNoche)) : null,
			habitaciones: propias
		};
	}

	private async publicar(tipo: TipoEventoCatalogo, habitacion: Habitacion, contexto: ContextoCambio, cambios?: { campo: string; antes: unknown; despues: unknown }[]): Promise<void> {
		const [hotel, ocupaciones] = await Promise.all([
			this.repositorio.obtenerHotel(habitacion.idHotel),
			this.repositorio.ocupaciones(habitacion.id, new Date())
		]);
		const evento = crearEventoHabitacion(tipo, habitacion, hotel, ocupaciones, contexto.actor, contexto.origen, cambios);
		try {
			await this.productor.publicar(evento);
		} catch (error) {
			log('error', 'EVENTO_CATALOGO_NO_PUBLICADO', { tipo, idHabitacion: habitacion.id, motivo: error instanceof Error ? error.message : String(error) });
			throw error;
		}
	}
}
