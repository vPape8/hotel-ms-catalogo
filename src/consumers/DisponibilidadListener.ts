import { kafkaConfig } from '../config/kafka.config';
import { log } from '../logs/logger';
import { ReservaEventoDto, validarEventoReserva } from '../messaging/eventoReserva';
import { ContextoRegistro, KafkaListener } from '../messaging/KafkaListener';
import { CatalogoService } from '../services/catalogoService';

/**
 * Reacción en tiempo real (F17): al llegar una reserva, el catálogo ocupa o libera una unidad de la habitación
 * y publica DISPONIBILIDAD_CAMBIADA en el tópico "catalogo", sin que nadie tenga que recargar nada.
 * Grupo "disponibilidad" con concurrency 3: la key es la habitación, así sus reservas se procesan en orden.
 */
export class DisponibilidadListener {
	constructor(private readonly servicio: CatalogoService) {}

	@KafkaListener({
		topics: [kafkaConfig.topicoReservas],
		groupId: kafkaConfig.grupos.disponibilidad,
		concurrency: kafkaConfig.concurrencia,
		validar: validarEventoReserva
	})
	async actualizarDisponibilidad(evento: ReservaEventoDto, contexto: ContextoRegistro): Promise<void> {
		const resultado = await this.servicio.reaccionarAReserva(evento);
		const datos = {
			eventoId: evento.eventoId,
			tipo: evento.tipo,
			codigo: evento.reserva.codigo,
			idHabitacion: evento.reserva.idHabitacion,
			particion: contexto.particion,
			offset: contexto.offset,
			miembro: contexto.miembro
		};
		if (resultado === 'HABITACION_DESCONOCIDA') {
			// No es un error del mensaje: la reserva es válida, solo que su habitación no está en el catálogo.
			log('warn', '[IGNORA]', { ...datos, detalle: 'la habitación no está en el catálogo' });
		} else if (resultado === 'SIN_CAMBIOS') {
			log('warn', '[IDEMP]', { ...datos, detalle: 'la ocupación ya estaba registrada, se ignora' });
		} else {
			log('info', '[REACCIONA]', { ...datos, estado: evento.reserva.estado });
		}
	}
}
