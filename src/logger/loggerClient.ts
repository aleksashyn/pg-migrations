import { Logger } from '../types';

export default class LoggerClient {
  private readonly logger: Logger | undefined;

  constructor(logger: Logger | undefined) {
    this.logger = logger;
  }

  info = (message: string): void => {
    if (this.logger != null) {
      this.logger.info(message);
    }
  };

  debug = (message: string): void => {
    if (this.logger != null) {
      this.logger.info(message);
    }
  };

  error = (message: string, error?: Error): void => {
    if (this.logger != null) {
      const errorMessage = `${message}${error != null ? `, cause: ${error.message}` : ''}`;
      return this.logger.error(errorMessage);
    }
  };
}
