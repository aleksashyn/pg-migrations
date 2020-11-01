import { QueryResult } from 'pg';
import { DatabaseClient } from '../database';
import { FsClient, MigrationItem } from '../fs';
import { LoggerClient } from '../logger';
import { MigrationConfig } from '../types';
import { MigrationEntry } from './types';

const CREATE_MIGRATIONS_TABLE = `CREATE TABLE migrations
                                 (
                                     id        BIGSERIAL PRIMARY KEY,
                                     filename  VARCHAR(255) NOT NULL UNIQUE,
                                     timestamp TIMESTAMP    NOT NULL DEFAULT now(),
                                     hash      VARCHAR(32)  NOT NULL
                                 );`;
const MIGRATIONS_TABLE_EXISTS = `SELECT EXISTS(SELECT 1 FROM pg_tables WHERE tablename = 'migrations')`;
const INSERT_MIGRATION_INIT_QUERY = 'INSERT INTO migrations (id, filename, hash) VALUES (0, $1, $2)';
const INSERT_MIGRATION_QUERY = 'INSERT INTO migrations (filename, hash) VALUES ($1, $2)';
const SELECT_EXISTING_MIGRATIONS = 'SELECT * FROM migrations';

interface TableExist {
  exists: boolean;
}

export default class PgMigrationClient {
  private readonly databaseClient: DatabaseClient;
  private readonly logger: LoggerClient;
  private readonly fsClient: FsClient;
  private readonly migrationPath: string;
  private initialized = false;

  constructor(config: MigrationConfig) {
    this.migrationPath = config.migrationsPath;
    this.logger = new LoggerClient(config.logger);
    this.databaseClient = new DatabaseClient(config.pool, this.logger);
    this.fsClient = new FsClient(this.logger);
  }

  init = async (): Promise<void> => {
    try {
      this.logger.info('Start initializing PgMigrationClient');
      const migrationsTableExist = await this.doExecuteQuery<TableExist>(MIGRATIONS_TABLE_EXISTS);
      const isMigrationsTableExist = migrationsTableExist?.rows[0].exists;
      this.logger.info('Check migrations table');
      if (isMigrationsTableExist) {
        this.logger.info('migrations table already exist');
        this.initialized = true;
        return;
      }
      this.logger.info('Unable to find migrations table');
      await this.databaseClient.execute(CREATE_MIGRATIONS_TABLE);
      await this.databaseClient.execute(INSERT_MIGRATION_INIT_QUERY, ['PgMigrationClient initialized', '0']);
      this.logger.info('Migrations table successfully created');
      this.initialized = true;
      this.logger.info('PgMigrationClient successfully initialized');
    } catch (error) {
      this.logger.error('Unable to start PgMigrationClient', error);
      this.initialized = false;
    }
  };

  private doExecuteQuery = async <R>(statement: string): Promise<QueryResult<R> | undefined> => {
    const result = await this.databaseClient.execute<R>(statement);
    if (result.isSuccess) {
      return result.result;
    } else {
      throw Error(result.error);
    }
  };

  startMigration = async (): Promise<void> => {
    try {
      this.logger.info('Start database migration');
      this.checkClientInitialisation();
      const migrationScripts = await this.fsClient.findMigrationScripts(this.migrationPath);
      if (migrationScripts.length === 0) {
        this.logger.info(`Migration scripts not found, path=${this.migrationPath}`);
      }
      const migrationEntries = await this.getPersistedMigrations();
      const missedMigrationScripts = migrationScripts.filter(
        (script) => migrationEntries.filter((entry) => script.hash === entry.hash).length === 0
      );
      if (missedMigrationScripts.length === 0) {
        this.logger.info('All migration scripts already applied');
      }
      for (const migrationScript of missedMigrationScripts) {
        await this.executeMigrationScript(migrationScript);
      }
      this.logger.info('Database migration complete successfully');
    } catch (error) {
      this.logger.error('Unable to complete database migration');
    }
  };

  private checkClientInitialisation = () => {
    if (!this.initialized) {
      throw new Error('PgMigrationClient should be initialized before use');
    }
  };

  private getPersistedMigrations = async () => {
    const queryResult = await this.doExecuteQuery<MigrationEntry>(SELECT_EXISTING_MIGRATIONS);
    return queryResult?.rows || [];
  };

  private executeMigrationScript = async (migrationScript: MigrationItem) => {
    const { filename, data, hash } = migrationScript;
    try {
      const useAutocommit = data.startsWith('--AUTOCOMMIT');
      if (useAutocommit) {
        this.logger.info('Execute migration script without transaction block');
        await this.doExecuteQuery(data);
      } else {
        await this.databaseClient.executeInTransaction(data);
      }
      await this.databaseClient.execute(INSERT_MIGRATION_QUERY, [filename, hash]);
      this.logger.info(`Script ${filename} complete successfully`);
    } catch (error) {
      this.logger.error(`Unable to execute script ${filename}`, error);
      throw error;
    }
  };
}
