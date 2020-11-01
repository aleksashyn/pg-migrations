import { createLogger } from 'bunyan';
import { Pool } from 'pg';
import { PgMigrationClient } from './migration';

export default PgMigrationClient;

const pool = new Pool({
  host: 'localhost',
  port: 5435,
  database: 'pg-migrations',
  user: 'postgres',
  password: 'postgres',
});

const logger = createLogger({
  name: 'pg-migrations',
});

const test = async () => {
  const pgMigrationClient = new PgMigrationClient({ pool, logger, migrationsPath: 'test/sql' });
  await pgMigrationClient.init();
  await pgMigrationClient.startMigration();
};

test();
