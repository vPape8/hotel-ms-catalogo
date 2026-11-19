import { ResultSetHeader, RowDataPacket } from 'mysql2';
import { Pool } from 'mysql2/promise';

export interface HotelInicial {
	id: string;
	slug: string;
	nombre: string;
	tipo: string;
	estrellas: number;
	barrio: string;
	direccion: string;
}

export interface HabitacionInicial {
	id: string;
	idHotel: string;
	nombre: string;
	descripcion: string;
	capacidad: number;
	camas: string;
	metrosCuadrados: number;
	precioPorNoche: number;
	/** Cuántas habitaciones de este tipo tiene el hotel. */
	unidades: number;
}

export type Hotel = HotelInicial;

export interface Habitacion extends HabitacionInicial {
	activa: boolean;
	actualizadaEn: Date;
}

/** Una reserva activa (PENDIENTE o CONFIRMADA) que ocupa una unidad de la habitación entre dos fechas. */
export interface Ocupacion {
	reservaId: string;
	idHabitacion: string;
	desde: Date;
	hasta: Date;
	estado: 'PENDIENTE' | 'CONFIRMADA';
}

export type CambiosHabitacion = Partial<Pick<Habitacion, 'nombre' | 'descripcion' | 'capacidad' | 'camas' | 'metrosCuadrados' | 'precioPorNoche' | 'unidades' | 'activa'>>;

export interface CatalogoRepository {
	/** Carga los datos iniciales si el catálogo está vacío. */
	inicializar(hoteles: HotelInicial[], habitaciones: HabitacionInicial[]): Promise<void>;
	listarHoteles(): Promise<Hotel[]>;
	obtenerHotel(id: string): Promise<Hotel | undefined>;
	listarHabitaciones(idHotel?: string): Promise<Habitacion[]>;
	obtenerHabitacion(id: string): Promise<Habitacion | undefined>;
	crearHabitacion(habitacion: Habitacion): Promise<Habitacion>;
	actualizarHabitacion(id: string, cambios: CambiosHabitacion): Promise<Habitacion | undefined>;
	/** Guarda o actualiza la ocupación de una reserva. Devuelve true si cambió algo. */
	registrarOcupacion(ocupacion: Ocupacion): Promise<boolean>;
	/** Quita la ocupación de una reserva (cancelada o completada). Devuelve true si existía. */
	liberarOcupacion(reservaId: string): Promise<boolean>;
	/** Ocupaciones de la habitación que terminan después de la fecha indicada. */
	ocupaciones(idHabitacion: string, desde: Date): Promise<Ocupacion[]>;
}

/** Para tests y desarrollo sin MySQL. */
export class InMemoryCatalogoRepository implements CatalogoRepository {
	readonly hoteles = new Map<string, Hotel>();
	readonly habitaciones = new Map<string, Habitacion>();
	readonly ocupacionesPorReserva = new Map<string, Ocupacion>();

	async inicializar(hoteles: HotelInicial[], habitaciones: HabitacionInicial[]): Promise<void> {
		if (this.hoteles.size > 0) return;
		for (const hotel of hoteles) this.hoteles.set(hotel.id, hotel);
		for (const habitacion of habitaciones) this.habitaciones.set(habitacion.id, { ...habitacion, activa: true, actualizadaEn: new Date() });
	}

	async listarHoteles(): Promise<Hotel[]> {
		return [...this.hoteles.values()];
	}

	async obtenerHotel(id: string): Promise<Hotel | undefined> {
		return this.hoteles.get(id);
	}

	async listarHabitaciones(idHotel?: string): Promise<Habitacion[]> {
		return [...this.habitaciones.values()].filter((habitacion) => !idHotel || habitacion.idHotel === idHotel);
	}

	async obtenerHabitacion(id: string): Promise<Habitacion | undefined> {
		return this.habitaciones.get(id);
	}

	async crearHabitacion(habitacion: Habitacion): Promise<Habitacion> {
		this.habitaciones.set(habitacion.id, habitacion);
		return habitacion;
	}

	async actualizarHabitacion(id: string, cambios: CambiosHabitacion): Promise<Habitacion | undefined> {
		const actual = this.habitaciones.get(id);
		if (!actual) return undefined;
		const actualizada = { ...actual, ...cambios, actualizadaEn: new Date() };
		this.habitaciones.set(id, actualizada);
		return actualizada;
	}

	async registrarOcupacion(ocupacion: Ocupacion): Promise<boolean> {
		const anterior = this.ocupacionesPorReserva.get(ocupacion.reservaId);
		this.ocupacionesPorReserva.set(ocupacion.reservaId, ocupacion);
		return !anterior || anterior.estado !== ocupacion.estado
			|| anterior.desde.getTime() !== ocupacion.desde.getTime() || anterior.hasta.getTime() !== ocupacion.hasta.getTime();
	}

	async liberarOcupacion(reservaId: string): Promise<boolean> {
		return this.ocupacionesPorReserva.delete(reservaId);
	}

	async ocupaciones(idHabitacion: string, desde: Date): Promise<Ocupacion[]> {
		return [...this.ocupacionesPorReserva.values()]
			.filter((ocupacion) => ocupacion.idHabitacion === idHabitacion && ocupacion.hasta > desde)
			.sort((a, b) => a.desde.getTime() - b.desde.getTime());
	}
}

const ESQUEMA = [
	`CREATE TABLE IF NOT EXISTS catalogo_hoteles (
		id VARCHAR(64) PRIMARY KEY,
		slug VARCHAR(150) NOT NULL UNIQUE,
		nombre VARCHAR(150) NOT NULL,
		tipo VARCHAR(50) NOT NULL,
		estrellas INT NOT NULL,
		barrio VARCHAR(100) NOT NULL,
		direccion VARCHAR(255) NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS catalogo_habitaciones (
		id VARCHAR(64) PRIMARY KEY,
		id_hotel VARCHAR(64) NOT NULL,
		nombre VARCHAR(150) NOT NULL,
		descripcion VARCHAR(500) NOT NULL,
		capacidad INT NOT NULL,
		camas VARCHAR(150) NOT NULL,
		metros_cuadrados INT NOT NULL,
		precio_por_noche DECIMAL(12, 2) NOT NULL,
		unidades INT NOT NULL,
		activa BOOLEAN NOT NULL DEFAULT TRUE,
		actualizada_en DATETIME(3) NOT NULL,
		INDEX idx_habitaciones_hotel (id_hotel),
		CONSTRAINT fk_habitacion_hotel FOREIGN KEY (id_hotel) REFERENCES catalogo_hoteles (id)
	)`,
	`CREATE TABLE IF NOT EXISTS catalogo_ocupaciones (
		reserva_id CHAR(36) PRIMARY KEY,
		id_habitacion VARCHAR(64) NOT NULL,
		desde DATETIME(3) NOT NULL,
		hasta DATETIME(3) NOT NULL,
		estado VARCHAR(20) NOT NULL,
		INDEX idx_ocupaciones_habitacion (id_habitacion, hasta)
	)`
];

interface FilaHotel extends RowDataPacket {
	id: string;
	slug: string;
	nombre: string;
	tipo: string;
	estrellas: number;
	barrio: string;
	direccion: string;
}

interface FilaHabitacion extends RowDataPacket {
	id: string;
	id_hotel: string;
	nombre: string;
	descripcion: string;
	capacidad: number;
	camas: string;
	metros_cuadrados: number;
	precio_por_noche: string;
	unidades: number;
	activa: number;
	actualizada_en: Date;
}

interface FilaOcupacion extends RowDataPacket {
	reserva_id: string;
	id_habitacion: string;
	desde: Date;
	hasta: Date;
	estado: 'PENDIENTE' | 'CONFIRMADA';
}

function aHotel(fila: FilaHotel): Hotel {
	return { id: fila.id, slug: fila.slug, nombre: fila.nombre, tipo: fila.tipo, estrellas: fila.estrellas, barrio: fila.barrio, direccion: fila.direccion };
}

function aHabitacion(fila: FilaHabitacion): Habitacion {
	return {
		id: fila.id,
		idHotel: fila.id_hotel,
		nombre: fila.nombre,
		descripcion: fila.descripcion,
		capacidad: fila.capacidad,
		camas: fila.camas,
		metrosCuadrados: fila.metros_cuadrados,
		precioPorNoche: Number(fila.precio_por_noche),
		unidades: fila.unidades,
		activa: Boolean(fila.activa),
		actualizadaEn: fila.actualizada_en
	};
}

const COLUMNAS_EDITABLES: Record<keyof CambiosHabitacion, string> = {
	nombre: 'nombre',
	descripcion: 'descripcion',
	capacidad: 'capacidad',
	camas: 'camas',
	metrosCuadrados: 'metros_cuadrados',
	precioPorNoche: 'precio_por_noche',
	unidades: 'unidades',
	activa: 'activa'
};

export class MySqlCatalogoRepository implements CatalogoRepository {
	private esquemaListo?: Promise<void>;

	constructor(private readonly pool: Pool) {}

	private asegurarEsquema(): Promise<void> {
		this.esquemaListo ??= (async () => {
			for (const sentencia of ESQUEMA) await this.pool.query(sentencia);
		})().catch((error: unknown) => {
			this.esquemaListo = undefined;
			throw error;
		});
		return this.esquemaListo;
	}

	async inicializar(hoteles: HotelInicial[], habitaciones: HabitacionInicial[]): Promise<void> {
		await this.asegurarEsquema();
		const [[conteo]] = await this.pool.query<RowDataPacket[]>('SELECT COUNT(*) AS total FROM catalogo_hoteles');
		if (Number(conteo?.total ?? 0) > 0) return;
		const ahora = new Date();
		await this.pool.query('INSERT IGNORE INTO catalogo_hoteles (id, slug, nombre, tipo, estrellas, barrio, direccion) VALUES ?',
			[hoteles.map((h) => [h.id, h.slug, h.nombre, h.tipo, h.estrellas, h.barrio, h.direccion])]);
		await this.pool.query(`INSERT IGNORE INTO catalogo_habitaciones
			(id, id_hotel, nombre, descripcion, capacidad, camas, metros_cuadrados, precio_por_noche, unidades, activa, actualizada_en) VALUES ?`,
			[habitaciones.map((h) => [h.id, h.idHotel, h.nombre, h.descripcion, h.capacidad, h.camas, h.metrosCuadrados, h.precioPorNoche, h.unidades, true, ahora])]);
	}

	async listarHoteles(): Promise<Hotel[]> {
		await this.asegurarEsquema();
		const [filas] = await this.pool.query<FilaHotel[]>('SELECT * FROM catalogo_hoteles ORDER BY id');
		return filas.map(aHotel);
	}

	async obtenerHotel(id: string): Promise<Hotel | undefined> {
		await this.asegurarEsquema();
		const [filas] = await this.pool.query<FilaHotel[]>('SELECT * FROM catalogo_hoteles WHERE id = ?', [id]);
		return filas[0] ? aHotel(filas[0]) : undefined;
	}

	async listarHabitaciones(idHotel?: string): Promise<Habitacion[]> {
		await this.asegurarEsquema();
		const [filas] = idHotel
			? await this.pool.query<FilaHabitacion[]>('SELECT * FROM catalogo_habitaciones WHERE id_hotel = ? ORDER BY id', [idHotel])
			: await this.pool.query<FilaHabitacion[]>('SELECT * FROM catalogo_habitaciones ORDER BY id');
		return filas.map(aHabitacion);
	}

	async obtenerHabitacion(id: string): Promise<Habitacion | undefined> {
		await this.asegurarEsquema();
		const [filas] = await this.pool.query<FilaHabitacion[]>('SELECT * FROM catalogo_habitaciones WHERE id = ?', [id]);
		return filas[0] ? aHabitacion(filas[0]) : undefined;
	}

	async crearHabitacion(h: Habitacion): Promise<Habitacion> {
		await this.asegurarEsquema();
		await this.pool.execute<ResultSetHeader>(
			`INSERT INTO catalogo_habitaciones
				(id, id_hotel, nombre, descripcion, capacidad, camas, metros_cuadrados, precio_por_noche, unidades, activa, actualizada_en)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			[h.id, h.idHotel, h.nombre, h.descripcion, h.capacidad, h.camas, h.metrosCuadrados, h.precioPorNoche, h.unidades, h.activa, h.actualizadaEn]
		);
		return h;
	}

	async actualizarHabitacion(id: string, cambios: CambiosHabitacion): Promise<Habitacion | undefined> {
		await this.asegurarEsquema();
		const campos = (Object.keys(cambios) as (keyof CambiosHabitacion)[]).filter((campo) => cambios[campo] !== undefined);
		if (campos.length > 0) {
			await this.pool.execute(
				`UPDATE catalogo_habitaciones SET ${campos.map((campo) => `${COLUMNAS_EDITABLES[campo]} = ?`).join(', ')}, actualizada_en = ? WHERE id = ?`,
				[...campos.map((campo) => cambios[campo] as string | number | boolean), new Date(), id]
			);
		}
		return this.obtenerHabitacion(id);
	}

	async registrarOcupacion(o: Ocupacion): Promise<boolean> {
		await this.asegurarEsquema();
		// affectedRows: 1 = nueva, 2 = cambió, 0 = era igual (reentrega del mismo evento).
		const [resultado] = await this.pool.execute<ResultSetHeader>(
			`INSERT INTO catalogo_ocupaciones (reserva_id, id_habitacion, desde, hasta, estado) VALUES (?, ?, ?, ?, ?) AS nueva
			ON DUPLICATE KEY UPDATE desde = nueva.desde, hasta = nueva.hasta, estado = nueva.estado`,
			[o.reservaId, o.idHabitacion, o.desde, o.hasta, o.estado]
		);
		return resultado.affectedRows > 0;
	}

	async liberarOcupacion(reservaId: string): Promise<boolean> {
		await this.asegurarEsquema();
		const [resultado] = await this.pool.execute<ResultSetHeader>('DELETE FROM catalogo_ocupaciones WHERE reserva_id = ?', [reservaId]);
		return resultado.affectedRows > 0;
	}

	async ocupaciones(idHabitacion: string, desde: Date): Promise<Ocupacion[]> {
		await this.asegurarEsquema();
		const [filas] = await this.pool.query<FilaOcupacion[]>(
			'SELECT * FROM catalogo_ocupaciones WHERE id_habitacion = ? AND hasta > ? ORDER BY desde', [idHabitacion, desde]);
		return filas.map((fila) => ({ reservaId: fila.reserva_id, idHabitacion: fila.id_habitacion, desde: fila.desde, hasta: fila.hasta, estado: fila.estado }));
	}
}
