import { CambiosHabitacion, HabitacionInicial } from '../repositories/catalogoRepository';

export interface ErrorValidacion {
	campo: string;
	mensaje: string;
}

export type Resultado<T> = { ok: true; valor: T } | { ok: false; errores: ErrorValidacion[] };

const ID = /^[a-z0-9][a-z0-9-]{2,63}$/;

interface ReglaNumero {
	minimo: number;
	maximo: number;
	entero: boolean;
}

const NUMEROS: Record<'capacidad' | 'metrosCuadrados' | 'precioPorNoche' | 'unidades', ReglaNumero> = {
	capacidad: { minimo: 1, maximo: 12, entero: true },
	metrosCuadrados: { minimo: 1, maximo: 500, entero: true },
	precioPorNoche: { minimo: 1, maximo: 10_000_000, entero: false },
	unidades: { minimo: 0, maximo: 100, entero: true }
};

const TEXTOS: Record<'nombre' | 'descripcion' | 'camas', number> = { nombre: 150, descripcion: 500, camas: 150 };

function esObjeto(valor: unknown): valor is Record<string, unknown> {
	return typeof valor === 'object' && valor !== null && !Array.isArray(valor);
}

function validarCampos(cuerpo: Record<string, unknown>, obligatorios: boolean, errores: ErrorValidacion[]): CambiosHabitacion {
	const resultado: CambiosHabitacion = {};
	for (const [campo, maximo] of Object.entries(TEXTOS) as [keyof typeof TEXTOS, number][]) {
		const valor = cuerpo[campo];
		if (valor === undefined && !obligatorios) continue;
		if (typeof valor !== 'string' || !valor.trim() || valor.length > maximo) {
			errores.push({ campo, mensaje: `Texto obligatorio de hasta ${maximo} caracteres` });
			continue;
		}
		resultado[campo] = valor.trim();
	}
	for (const [campo, regla] of Object.entries(NUMEROS) as [keyof typeof NUMEROS, ReglaNumero][]) {
		const valor = cuerpo[campo];
		if (valor === undefined && !obligatorios) continue;
		if (typeof valor !== 'number' || !Number.isFinite(valor) || valor < regla.minimo || valor > regla.maximo || (regla.entero && !Number.isInteger(valor))) {
			errores.push({ campo, mensaje: `Debe ser ${regla.entero ? 'un entero' : 'un número'} entre ${regla.minimo} y ${regla.maximo}` });
			continue;
		}
		resultado[campo] = valor;
	}
	return resultado;
}

function camposDesconocidos(cuerpo: Record<string, unknown>, permitidos: string[], errores: ErrorValidacion[]): void {
	for (const campo of Object.keys(cuerpo)) {
		if (!permitidos.includes(campo)) {
			errores.push({ campo, mensaje: `Campo no permitido. Usa: ${permitidos.join(', ')}` });
		}
	}
}

/** POST /api/v1/catalogo/habitaciones */
export function validarCrearHabitacion(cuerpo: unknown): Resultado<HabitacionInicial> {
	if (!esObjeto(cuerpo)) return { ok: false, errores: [{ campo: 'body', mensaje: 'Se esperaba un objeto JSON' }] };
	const errores: ErrorValidacion[] = [];
	camposDesconocidos(cuerpo, ['id', 'idHotel', ...Object.keys(TEXTOS), ...Object.keys(NUMEROS)], errores);
	const id = typeof cuerpo.id === 'string' && ID.test(cuerpo.id) ? cuerpo.id : undefined;
	if (!id) errores.push({ campo: 'id', mensaje: 'Minúsculas, números y guiones (3 a 64), por ejemplo vpo-001-room-5' });
	const idHotel = typeof cuerpo.idHotel === 'string' && cuerpo.idHotel.trim() ? cuerpo.idHotel.trim() : undefined;
	if (!idHotel) errores.push({ campo: 'idHotel', mensaje: 'Es obligatorio' });
	const campos = validarCampos(cuerpo, true, errores);
	return errores.length > 0
		? { ok: false, errores }
		: { ok: true, valor: { id: id!, idHotel: idHotel!, ...campos } as HabitacionInicial };
}

/** PATCH /api/v1/catalogo/habitaciones/{id}: cualquiera de los campos editables, al menos uno. */
export function validarActualizarHabitacion(cuerpo: unknown): Resultado<CambiosHabitacion> {
	if (!esObjeto(cuerpo)) return { ok: false, errores: [{ campo: 'body', mensaje: 'Se esperaba un objeto JSON' }] };
	const errores: ErrorValidacion[] = [];
	camposDesconocidos(cuerpo, [...Object.keys(TEXTOS), ...Object.keys(NUMEROS), 'activa'], errores);
	const cambios = validarCampos(cuerpo, false, errores);
	if (cuerpo.activa !== undefined) {
		if (typeof cuerpo.activa !== 'boolean') errores.push({ campo: 'activa', mensaje: 'Debe ser true o false' });
		else cambios.activa = cuerpo.activa;
	}
	if (errores.length === 0 && Object.keys(cambios).length === 0) {
		errores.push({ campo: 'body', mensaje: 'Indica al menos un campo para cambiar' });
	}
	return errores.length > 0 ? { ok: false, errores } : { ok: true, valor: cambios };
}
