import { RequestHandler, Router } from 'express';
import { CatalogoController } from '../controllers/catalogoController';
import { CatalogoService } from '../services/catalogoService';

/**
 * Rutas del catálogo:
 *  - Lectura pública (sin token): el visitante ve hoteles, habitaciones, precios y disponibilidad.
 *  - Crear y editar habitaciones: solo Admin (JWT de Entra ID con el app role ADMIN).
 */
export function crearCatalogoRoutes(servicio: CatalogoService, autenticacionAdmin: RequestHandler): Router {
	const router = Router();
	const controlador = new CatalogoController(servicio);

	router.get('/catalogo/hoteles', controlador.listarHoteles);
	router.get('/catalogo/hoteles/:id', controlador.obtenerHotel);
	router.get('/catalogo/habitaciones/:id', controlador.obtenerHabitacion);
	router.get('/catalogo/habitaciones/:id/disponibilidad', controlador.disponibilidad);

	router.post('/catalogo/habitaciones', autenticacionAdmin, controlador.crearHabitacion);
	router.patch('/catalogo/habitaciones/:id', autenticacionAdmin, controlador.actualizarHabitacion);

	return router;
}
