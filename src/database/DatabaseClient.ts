/* eslint-disable @typescript-eslint/no-explicit-any */
import { Pool, QueryResult, QueryResultRow } from 'pg';
import { LoggerClient } from '../logger';
import { ExecutionResult } from './types';

export default class DatabaseClient {
  private readonly pool: Pool;
  private readonly logger: LoggerClient;

  constructor(pool: Pool, logger: LoggerClient) {
    this.pool = pool;
    this.logger = logger;
  }

  execute = async <R extends QueryResultRow>(statement: string, params?: any[]): Promise<ExecutionResult<R>> => {
    try {
      const queryResult: QueryResult<R> = await this.pool.query(statement, params);
      return this.toSuccess(queryResult);
    } catch (error) {
      return this.toError(error);
    }
  };

  executeInTransaction = async <R extends QueryResultRow>(statement: string): Promise<ExecutionResult<R>> => {
    const client = await this.pool.connect();
    try {
      this.logger.debug('Statement execution started');
      await client.query('START TRANSACTION');
      const queryResult = await client.query(statement);
      await client.query('COMMIT');
      this.logger.debug('The Statement was executed successfully');
      return this.toSuccess(queryResult);
    } catch (error) {
      await client.query('ROLLBACK');
      this.logger.error('Statement execution failed', error);
      return this.toError(error);
    } finally {
      client.release();
    }
  };

  private toSuccess = <R extends QueryResultRow>(queryResult: QueryResult<R>) => ({
    isSuccess: true,
    result: queryResult,
  });

  private toError = (error: Error) => ({
    isSuccess: false,
    error: error.message,
  });
}
