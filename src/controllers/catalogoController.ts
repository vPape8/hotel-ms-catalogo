import { Request, Response } from 'express';
import { log } from '../logs/logger';
import {
	CatalogoService,
	CatalogoValidacionError,
	ContextoCambio,
	HabitacionDuplicadaError,
	RecursoNoEncontradoError
} from '../services/catalogoService';
import { ErrorValidacion, validarActualizarHabitacion, validarCrearHabitacion } from '../services/validarHabitacion';

function responderValidacion(respuesta: Response, detalles: ErrorValidacion[]): void {
	respuesta.status(400).json({ error: 'VALIDACION', detalles });
}

function responderError(respuesta: Response, error: unknown): void {
	if (error instanceof CatalogoValidacionError) {
		responderValidacion(respuesta, error.detalles);
	} else if (error instanceof RecursoNoEncontradoError) {
		respuesta.status(404).json({ error: 'RECURSO_NO_ENCONTRADO', mensaje: error.message });
	} else if (error instanceof HabitacionDuplicadaError) {
		respuesta.status(409).json({ error: 'HABITACION_DUPLICADA', mensaje: error.message });
	} else {
		log('error', 'ERROR_CATALOGO', { motivo: error instanceof Error ? error.message : String(error) });
		respuesta.status(500).json({ error: 'ERROR_INTERNO' });
	}
}

function fecha(valor: unknown): Date | undefined {
	return typeof valor === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(valor) && !Number.isNaN(Date.parse(valor))
		? new Date(`${valor}T00:00:00.000Z`)
		: undefined;
}

/** Quién hace el cambio (del token de Entra ID) y desde dónde: viaja en el evento para la auditoría. */
function contextoAdmin(solicitud: Request): ContextoCambio {
	const claims = solicitud.user ?? {};
	const id = [claims.oid, claims.sub, claims.preferred_username].find((valor) => typeof valor === 'string') as string | undefined;
	const roles = Array.isArray(claims.roles) ? claims.roles.filter((rol): rol is string => typeof rol === 'string') : [];
	const ip = solicitud.header('x-forwarded-for')?.split(',')[0]?.trim() || solicitud.socket.remoteAddress;
	return {
		actor: { tipo: 'INTERNO', id: id ?? 'desconocido', ...(typeof claims.name === 'string' ? { nombre: claims.name } : {}), roles },
		origen: { canal: 'PANEL_ADMIN', ...(ip ? { ip } : {}) }
	};
}

export class CatalogoController {
	constructor(private readonly servicio: CatalogoService) {}

	/** GET /api/v1/catalogo/hoteles (pública). */
	listarHoteles = async (_solicitud: Request, respuesta: Response): Promise<void> => {
		try {
			respuesta.json({ data: await this.servicio.listarHoteles() });
		} catch (error) {
			responderError(respuesta, error);
		}
	};

	/** GET /api/v1/catalogo/hoteles/{id} (pública). */
	obtenerHotel = async (solicitud: Request, respuesta: Response): Promise<void> => {
		try {
			respuesta.json({ data: await this.servicio.obtenerHotel(String(solicitud.params.id)) });
		} catch (error) {
			responderError(respuesta, error);
		}
	};

	/** GET /api/v1/catalogo/habitaciones/{id} (pública). */
	obtenerHabitacion = async (solicitud: Request, respuesta: Response): Promise<void> => {
		try {
			respuesta.json({ data: await this.servicio.obtenerHabitacion(String(solicitud.params.id)) });
		} catch (error) {
			responderError(respuesta, error);
		}
	};

	/** GET /api/v1/catalogo/habitaciones/{id}/disponibilidad?desde=YYYY-MM-DD&hasta=YYYY-MM-DD (pública). */
	disponibilidad = async (solicitud: Request, respuesta: Response): Promise<void> => {
		const desde = fecha(solicitud.query.desde);
		const hasta = fecha(solicitud.query.hasta);
		const errores: ErrorValidacion[] = [];
		if (!desde) errores.push({ campo: 'desde', mensaje: 'Debe ser una fecha con formato YYYY-MM-DD' });
		if (!hasta) errores.push({ campo: 'hasta', mensaje: 'Debe ser una fecha con formato YYYY-MM-DD' });
		if (desde && hasta && hasta <= desde) errores.push({ campo: 'hasta', mensaje: 'Debe ser posterior a desde' });
		if (errores.length > 0) {
			responderValidacion(respuesta, errores);
			return;
		}
		try {
			respuesta.json({ data: await this.servicio.disponibilidad(String(solicitud.params.id), desde!, hasta!) });
		} catch (error) {
			responderError(respuesta, error);
		}
	};

	/** POST /api/v1/catalogo/habitaciones (Admin). */
	crearHabitacion = async (solicitud: Request, respuesta: Response): Promise<void> => {
		const validacion = validarCrearHabitacion(solicitud.body);
		if (!validacion.ok) {
			responderValidacion(respuesta, validacion.errores);
			return;
		}
		try {
			respuesta.status(201).json({ data: await this.servicio.crearHabitacion(validacion.valor, contextoAdmin(solicitud)) });
		} catch (error) {
			responderError(respuesta, error);
		}
	};

	/** PATCH /api/v1/catalogo/habitaciones/{id} (Admin): precio, unidades, datos o activar/desactivar. */
	actualizarHabitacion = async (solicitud: Request, respuesta: Response): Promise<void> => {
		const validacion = validarActualizarHabitacion(solicitud.body);
		if (!validacion.ok) {
			responderValidacion(respuesta, validacion.errores);
			return;
		}
		try {
			respuesta.json({ data: await this.servicio.actualizarHabitacion(String(solicitud.params.id), validacion.valor, contextoAdmin(solicitud)) });
		} catch (error) {
			responderError(respuesta, error);
		}
	};
}
