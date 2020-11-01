import crypto from 'crypto';
import fs from 'fs';
import path from 'path';
import util from 'util';
import { LoggerClient } from '../logger';
import { MigrationItem } from './types';

export default class FsClient {
  private readonly logger: LoggerClient;
  private readonly readDir = util.promisify(fs.readdir);
  private readonly readFile = util.promisify(fs.readFile);

  constructor(logger: LoggerClient) {
    this.logger = logger;
  }

  private createMigrationItem = async (dirName: string, filename: string): Promise<MigrationItem> => {
    const filePath = path.join(dirName, filename);
    const data = await this.readFile(filePath, 'utf-8');
    const hash = crypto.createHash('md5').update(filename).update(data).digest('hex');
    return {
      filename,
      data,
      hash,
    };
  };

  findMigrationScripts = async (dirPath: string): Promise<MigrationItem[]> => {
    const result = [];
    this.logger.debug(`Searching for migration scripts in the directory started, dir=${dirPath}`);
    try {
      const files = await this.readDir(dirPath);
      for (const file of files) {
        const migrationItem = await this.createMigrationItem(dirPath, file);
        result.push(migrationItem);
      }
    } catch (error) {
      this.logger.error('Unable to find migration scripts');
      throw error;
    }
    this.logger.debug(
      `Search for migration scripts in the directory completed successfully, found ${result.length} script(-s)`
    );
    return result;
  };
}
