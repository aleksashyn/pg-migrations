import { QueryResult } from 'pg';
import { DatabaseClient, ExecutionResult } from '../database';
import { FsClient, MigrationItem } from '../fs';
import { LoggerClient } from '../logger';
import { MigrationConfig } from '../types';
import { MigrationEntry } from './types';

const CREATE_MIGRATIONS_TABLE = `CREATE TABLE migrations
                                 (
                                     id        BIGSERIAL PRIMARY KEY,
                                     filename  VARCHAR(255) NOT NULL UNIQUE,
                                     timestamp TIMESTAMP    NOT NULL DEFAULT now(),
                                     hash      VARCHAR(32)
                                 );`;
const MIGRATIONS_TABLE_EXISTS = `SELECT EXISTS(SELECT 1 FROM pg_tables WHERE tablename = 'migrations')`;
const INSERT_MIGRATION_INIT_QUERY = 'INSERT INTO migrations (id, filename) VALUES (0, $1)';
const INSERT_MIGRATION_QUERY = 'INSERT INTO migrations (filename, hash) VALUES ($1, $2)';
const SELECT_EXISTING_MIGRATIONS = 'SELECT * FROM migrations ORDER BY id';
const UPDATE_MIGRATION_ROW_HASH = 'UPDATE migrations SET hash = $1 WHERE filename = $2';

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
        this.logger.info('Migrations table already exist');
        this.initialized = true;
        return;
      }
      this.logger.info('Unable to find migrations table');
      await this.doExecuteQuery(CREATE_MIGRATIONS_TABLE);
      await this.databaseClient.execute(INSERT_MIGRATION_INIT_QUERY, ['PgMigrationClient initialized']);
      this.logger.info('Migrations table successfully created');
      this.initialized = true;
      this.logger.info('PgMigrationClient successfully initialized');
    } catch (error) {
      this.logger.error('Unable to start PgMigrationClient', error);
      this.initialized = false;
    }
  };

  startMigration = async (): Promise<void> => {
    try {
      this.logger.info('Start database migration');
      this.checkClientInitialisation();
      const migrationScripts = await this.getMigrationsScripts();
      if (migrationScripts.length === 0) {
        this.logger.info(`Migration scripts not found, path=${this.migrationPath}`);
      } else {
        await this.doMigration(migrationScripts);
      }
      this.logger.info('Database migration complete successfully');
    } catch (error) {
      this.logger.error('Unable to complete database migration', error);
    }
  };

  private checkClientInitialisation = () => {
    if (!this.initialized) {
      throw new Error('PgMigrationClient should be initialized before use');
    }
  };

  private getMigrationsScripts = async (): Promise<MigrationItem[]> => {
    const migrationScripts = await this.fsClient.findMigrationScripts(this.migrationPath);
    migrationScripts.sort((left, right) => left.id - right.id);
    let currentId: number;
    migrationScripts.forEach((script) => {
      if (currentId != null && script.id - currentId !== 1) {
        throw new Error(`Invalid script IDs sequence for filename ${script.filename}`);
      }
      currentId = script.id;
    });
    return migrationScripts;
  };

  private doMigration = async (migrationScripts: MigrationItem[]) => {
    const migrationEntries = await this.getPersistedMigrations();
    const missedMigrationScripts: MigrationItem[] = [];
    let lastMigrationEntryId: number = migrationEntries[migrationEntries.length - 1].id;
    for (const migrationScript of migrationScripts) {
      const migrationEntry = migrationEntries.find((entry) => entry.filename === migrationScript.filename);
      if (migrationEntry == null) {
        missedMigrationScripts.push(migrationScript);
        await this.validateScriptId(migrationScript, lastMigrationEntryId);
        await this.executeMigrationScript(migrationScript);
        lastMigrationEntryId = migrationScript.id;
      } else {
        await this.updateScriptHash(migrationScript, migrationEntry.hash);
      }
    }
    if (missedMigrationScripts.length === 0) {
      this.logger.info('All migration scripts already applied');
    }
  };

  private validateScriptId = (migrationScript: MigrationItem, lastMigrationEntryId: number) => {
    if (migrationScript.id - lastMigrationEntryId !== 1) {
      throw new Error(
        `Invalid script ID for filename ${migrationScript.filename}, expected ID=${+lastMigrationEntryId + 1}`
      );
    }
  };

  private executeMigrationScript = async (migrationScript: MigrationItem) => {
    const { filename, data, hash } = migrationScript;
    try {
      const useAutocommit = data.startsWith('--AUTOCOMMIT');
      let migrationResult: ExecutionResult<never>;
      if (useAutocommit) {
        this.logger.info('Execute migration script without transaction block');
        migrationResult = await this.databaseClient.execute(data);
      } else {
        migrationResult = await this.databaseClient.executeInTransaction(data);
      }
      await this.addMigrationEntry(migrationResult, filename, hash);
      this.logger.info(`Script ${filename} complete successfully`);
    } catch (error) {
      this.logger.error(`Unable to execute script ${filename}`, error);
      throw error;
    }
  };

  private addMigrationEntry = async (migrationResult: ExecutionResult<never>, filename: string, hash: string) => {
    if (migrationResult.isSuccess) {
      await this.databaseClient.execute(INSERT_MIGRATION_QUERY, [filename, hash]);
    } else {
      throw new Error(migrationResult.error);
    }
  };

  private updateScriptHash = async (migrationScript: MigrationItem, entryHash: string) => {
    if (entryHash == null) {
      await this.databaseClient.execute(UPDATE_MIGRATION_ROW_HASH, [migrationScript.hash, migrationScript.filename]);
    } else if (entryHash !== migrationScript.hash) {
      throw new Error(`For migration script '${migrationScript.filename}' was modify that not allowed`);
    }
  };

  private getPersistedMigrations = async () => {
    const queryResult = await this.doExecuteQuery<MigrationEntry>(SELECT_EXISTING_MIGRATIONS);
    return queryResult?.rows || [];
  };

  private doExecuteQuery = async <R>(statement: string): Promise<QueryResult<R> | undefined> => {
    const result = await this.databaseClient.execute<R>(statement);
    if (result.isSuccess) {
      return result.result;
    } else {
      throw Error(result.error);
    }
  };
}
