import assert from 'node:assert/strict';
import { AddressInfo } from 'node:net';
import test from 'node:test';
import { RequestHandler } from 'express';
import { crearApp } from './app';
import { HABITACIONES_INICIALES, HOTELES_INICIALES } from './config/datosIniciales';
import { DisponibilidadListener } from './consumers/DisponibilidadListener';
import { HabitacionEventoDto } from './messaging/eventoCatalogo';
import { ReservaEventoDto } from './messaging/eventoReserva';
import { listenersKafkaRegistrados } from './messaging/KafkaListener';
import { ProductorEventosCatalogo, serializarEvento } from './messaging/productorEventosCatalogo';
import { InMemoryCatalogoRepository } from './repositories/catalogoRepository';
import { CatalogoService } from './services/catalogoService';

class ProductorFalso implements ProductorEventosCatalogo {
	readonly eventos: HabitacionEventoDto[] = [];
	async publicar(evento: HabitacionEventoDto): Promise<void> { this.eventos.push(evento); }
	async iniciar(): Promise<void> {}
	async cerrar(): Promise<void> {}
}

const HABITACION = 'vpo-001-room-1';

function reserva(id: string, tipo: ReservaEventoDto['tipo'], estado: ReservaEventoDto['reserva']['estado'], idHabitacion = HABITACION): ReservaEventoDto {
	return {
		eventoId: `${id}-${tipo}`,
		tipo,
		version: 1,
		ocurridoEn: '2026-11-20T10:00:00.000Z',
		actor: { tipo: 'INVITADO', id: 'invitado', roles: [] },
		origen: { canal: 'WEB_HUESPEDES' },
		reserva: {
			id, codigo: `RES-${id}`, idHabitacion, estado,
			fechaEntrada: '2030-12-01T15:00:00.000Z', fechaSalida: '2030-12-03T11:00:00.000Z',
			noches: 2, cantidadHuespedes: 2, precioTotal: 496000
		}
	};
}

const contexto = { topico: 'reservas', particion: 0, offset: '0', headers: {}, miembro: 0 };

async function preparar() {
	const repositorio = new InMemoryCatalogoRepository();
	await repositorio.inicializar(HOTELES_INICIALES, HABITACIONES_INICIALES);
	const productor = new ProductorFalso();
	const servicio = new CatalogoService(repositorio, productor);
	return { repositorio, productor, servicio };
}

test('los datos iniciales son los mismos del frontend de huéspedes', async () => {
	const { repositorio } = await preparar();
	assert.equal(HOTELES_INICIALES.length, 24);
	assert.equal((await repositorio.obtenerHabitacion(HABITACION))?.unidades, 2);
	assert.ok(HABITACIONES_INICIALES.every((habitacion) => HOTELES_INICIALES.some((hotel) => hotel.id === habitacion.idHotel)));
});

test('reacción: una reserva ocupa una unidad, la repetida no cambia nada y la cancelación la libera', async () => {
	const { servicio, productor } = await preparar();
	new DisponibilidadListener(servicio);
	const listener = listenersKafkaRegistrados().find((registrado) => registrado.groupId === 'disponibilidad')!;
	assert.equal(listener.concurrency, 3);
	const desde = new Date('2030-12-01T00:00:00Z');
	const hasta = new Date('2030-12-02T00:00:00Z');

	await listener.manejar(reserva('r1', 'RESERVA_CREADA', 'PENDIENTE'), contexto);
	let disponibilidad = await servicio.disponibilidad(HABITACION, desde, hasta);
	assert.deepEqual([disponibilidad.ocupadas, disponibilidad.disponibles], [1, 1]);

	await listener.manejar(reserva('r1', 'RESERVA_CREADA', 'PENDIENTE'), contexto);
	await listener.manejar(reserva('r2', 'RESERVA_CREADA', 'PENDIENTE'), contexto);
	disponibilidad = await servicio.disponibilidad(HABITACION, desde, hasta);
	assert.equal(disponibilidad.disponible, false);

	await listener.manejar(reserva('r1', 'RESERVA_CANCELADA', 'CANCELADA'), contexto);
	assert.equal((await servicio.disponibilidad(HABITACION, desde, hasta)).disponibles, 1);

	// 3 cambios reales (r1 creada, r2 creada, r1 cancelada) = 3 eventos; la reentrega de r1 no publica nada.
	assert.deepEqual(productor.eventos.map((evento) => evento.tipo), ['DISPONIBILIDAD_CAMBIADA', 'DISPONIBILIDAD_CAMBIADA', 'DISPONIBILIDAD_CAMBIADA']);
	const ultimo = productor.eventos.at(-1)!;
	assert.deepEqual(ultimo.actor, { tipo: 'SISTEMA', id: 'hotel-ms-catalogo', roles: [] });
	assert.equal(ultimo.origen.codigoReserva, 'RES-r1');
	assert.deepEqual(ultimo.habitacion.ocupaciones.map((o) => o.reservaId), ['r2']);
	assert.equal(serializarEvento(ultimo).key, HABITACION);

	// Una habitación que no está en el catálogo se ignora (no es un mensaje malo).
	assert.equal(await servicio.reaccionarAReserva(reserva('r9', 'RESERVA_CREADA', 'PENDIENTE', 'HAB-999')), 'HABITACION_DESCONOCIDA');
});

test('API: lectura pública, edición solo Admin, con validación y evento de auditoría', async (t) => {
	const { servicio, productor } = await preparar();
	const autenticacionAdmin: RequestHandler = (solicitud, respuesta, siguiente) => {
		if (solicitud.header('authorization') !== 'Bearer admin') {
			respuesta.status(401).json({ error: 'TOKEN_REQUERIDO' });
			return;
		}
		solicitud.user = { oid: 'oid-admin', name: 'Admin Hotel', roles: ['ADMIN'] };
		siguiente();
	};
	const servidor = crearApp({ servicio, autenticacionAdmin }).listen(0);
	await new Promise<void>((resolve) => servidor.once('listening', resolve));
	t.after(() => servidor.close());
	const base = `http://127.0.0.1:${(servidor.address() as AddressInfo).port}/api/v1/catalogo`;
	const json = (metodo: string, cuerpo: unknown, token?: string) => ({
		method: metodo,
		headers: { 'Content-Type': 'application/json', ...(token ? { Authorization: `Bearer ${token}` } : {}) },
		body: JSON.stringify(cuerpo)
	});

	const hoteles = await (await fetch(`${base}/hoteles`)).json() as { data: { id: string; precioDesde: number; habitaciones: unknown[] }[] };
	assert.equal(hoteles.data.length, 24);
	assert.equal(hoteles.data[0]?.precioDesde, 248000);
	assert.equal((await fetch(`${base}/hoteles/vpo-001`)).status, 200);
	assert.equal((await fetch(`${base}/hoteles/no-existe`)).status, 404);
	assert.equal((await fetch(`${base}/habitaciones/${HABITACION}/disponibilidad?desde=2030-12-01&hasta=2030-12-03`)).status, 200);
	assert.equal((await fetch(`${base}/habitaciones/${HABITACION}/disponibilidad?desde=2030-12-03&hasta=2030-12-01`)).status, 400);

	assert.equal((await fetch(`${base}/habitaciones/${HABITACION}`, json('PATCH', { precioPorNoche: 260000 }))).status, 401);
	assert.equal((await fetch(`${base}/habitaciones/${HABITACION}`, json('PATCH', { precioPorNoche: -5 }, 'admin'))).status, 400);
	assert.equal((await fetch(`${base}/habitaciones/${HABITACION}`, json('PATCH', {}, 'admin'))).status, 400);
	const editada = await fetch(`${base}/habitaciones/${HABITACION}`, json('PATCH', { precioPorNoche: 260000, unidades: 3 }, 'admin'));
	assert.equal(editada.status, 200);
	await new Promise((resolve) => setImmediate(resolve));
	const evento = productor.eventos.find((e) => e.tipo === 'HABITACION_ACTUALIZADA')!;
	assert.equal(evento.actor.id, 'oid-admin');
	assert.deepEqual(evento.cambios, [
		{ campo: 'precioPorNoche', antes: 248000, despues: 260000 },
		{ campo: 'unidades', antes: 2, despues: 3 }
	]);

	const nueva = { id: 'vpo-001-room-9', idHotel: 'vpo-001', nombre: 'Suite presidencial', descripcion: 'Última planta con terraza.', capacidad: 2, camas: '1 cama king', metrosCuadrados: 60, precioPorNoche: 500000, unidades: 1 };
	assert.equal((await fetch(`${base}/habitaciones`, json('POST', nueva, 'admin'))).status, 201);
	assert.equal((await fetch(`${base}/habitaciones`, json('POST', nueva, 'admin'))).status, 409);
	assert.equal((await fetch(`${base}/habitaciones`, json('POST', { ...nueva, id: 'vpo-999-room-1', idHotel: 'vpo-999' }, 'admin'))).status, 400);
	assert.equal((await fetch(`${base}/habitaciones`, json('POST', { ...nueva, id: 'X' }, 'admin'))).status, 400);
});
