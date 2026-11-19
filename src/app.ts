import express, { Express, NextFunction, Request, RequestHandler, Response } from 'express';
import { entraAuthMiddleware } from './middlewares/authMiddleware';
import { crearCatalogoRoutes } from './routes/catalogoRoutes';
import { CatalogoService } from './services/catalogoService';

export interface OpcionesApp {
	servicio: CatalogoService;
	autenticacionAdmin?: RequestHandler;
	kafkaConectado?: () => boolean;
}

export function crearApp(opciones: OpcionesApp): Express {
	const app = express();
	app.use(express.json());

	app.get('/health', (_solicitud, respuesta) => {
		respuesta.json({ status: 'ok', kafka: opciones.kafkaConectado?.() ? 'conectado' : 'desconectado' });
	});

	app.use('/api/v1', crearCatalogoRoutes(opciones.servicio, opciones.autenticacionAdmin ?? entraAuthMiddleware));

	app.use((_solicitud, respuesta) => {
		respuesta.status(404).json({ error: 'RUTA_NO_ENCONTRADA' });
	});

	app.use((error: unknown, _solicitud: Request, respuesta: Response, _siguiente: NextFunction) => {
		if (error instanceof SyntaxError) {
			respuesta.status(400).json({ error: 'JSON_INVALIDO' });
			return;
		}
		respuesta.status(500).json({ error: 'ERROR_INTERNO' });
	});

	return app;
}
