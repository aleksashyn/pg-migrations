import { QueryResult } from 'pg';

export interface ExecutionResult<T> {
  isSuccess: boolean;
  result?: QueryResult<T> | undefined;
  error?: string | undefined;
}
