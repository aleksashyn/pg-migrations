/* eslint-disable @typescript-eslint/no-explicit-any */
import { Pool } from 'pg';

export interface Logger {
  info: (message: string) => void;
  debug: (message: string) => void;
  error: (message: string) => void;
}

export interface MigrationConfig {
  pool: Pool;
  logger?: Logger | undefined;
  migrationsPath: string;
}
