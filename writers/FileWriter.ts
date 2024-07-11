/* * */

import LOGGER from '@helperkits/logger';
import TIMETRACKER from '@helperkits/timer';
import fs from 'node:fs';
import Papa from 'papaparse';

/* * */

interface FileWriterOptions {
	batch_size?: number
	new_line_character?: string
}

/* * */

export default class FileWriter {
	//

	private CURRENT_BATCH_DATA = [];

	private FILE_PATH = null;

	private INSTANCE_NAME = 'Unnamed Instance';

	private MAX_BATCH_SIZE = 3000;

	private NEW_LINE_CHARACTER = '\n';

	private SESSION_TIMER = new TIMETRACKER();

	/* * */

	constructor(instanceName: string, filePath, options?: FileWriterOptions) {
		if (instanceName) this.INSTANCE_NAME = instanceName;
		if (filePath) this.FILE_PATH = filePath;
		if (options?.batch_size) this.MAX_BATCH_SIZE = options.batch_size;
		if (options?.new_line_character) this.NEW_LINE_CHARACTER = options.new_line_character;
	}

	/* * */

	async flush() {
		return new Promise<void>((resolve, reject) => {
			try {
				//

				if (!this.FILE_PATH) {
					return resolve();
				}

				const flushTimer = new TIMETRACKER();
				const sssionTimerResult = this.SESSION_TIMER.get();

				if (this.CURRENT_BATCH_DATA.length === 0) return;

				// Setup a variable to keep track if the file exists or not
				let fileAlreadyExists = true;

				// Try to access the file and append data to it
				fs.access(this.FILE_PATH, fs.constants.F_OK, async (error) => {
					//
					// If an error is thrown, then the file does not exist
					if (error) {
						fileAlreadyExists = false;
					}

					// Use papaparse to produce the CSV string
					let csvData = Papa.unparse(this.CURRENT_BATCH_DATA, { header: !fileAlreadyExists, newline: this.NEW_LINE_CHARACTER, skipEmptyLines: 'greedy' });

					// Prepend a new line character to csvData string if it is not the first line on the file
					if (fileAlreadyExists) {
						csvData = this.NEW_LINE_CHARACTER + csvData;
					}

					// Append the csv string to the file
					fs.appendFile(this.FILE_PATH, csvData, (appendErr) => {
						if (appendErr) {
							reject(new Error(`Error appending data to file: ${appendErr.message}`));
						}
						else {
							this.CURRENT_BATCH_DATA = [];
							LOGGER.info(`FILEWRITER [${this.INSTANCE_NAME}]: Flush | Length: ${this.CURRENT_BATCH_DATA.length} | File Path: ${this.FILE_PATH} (session: ${sssionTimerResult}) (flush: ${flushTimer.get()})`);
							resolve();
						}
					});

					//
				});

				//
			}
			catch (error) {
				reject(new Error(`Error at flush(): ${error.message}`));
			}
		});
	}

	/* * */

	async write(workdir, filename, data) {
		// Check if the batch workdir is the same of the current operation
		if (this.FILE_PATH !== `${workdir}/${filename}`) {
			await this.flush();
		}

		// Check if the batch is full
		if (this.CURRENT_BATCH_DATA.length >= this.MAX_BATCH_SIZE) {
			await this.flush();
		}

		// Reset the timer
		if (this.CURRENT_BATCH_DATA.length === 0) {
			this.SESSION_TIMER.reset();
		}

		// Set the working dir
		this.FILE_PATH = `${workdir}/${filename}`;

		// Add the data to the batch
		if (Array.isArray(data)) {
			this.CURRENT_BATCH_DATA = [...this.CURRENT_BATCH_DATA, ...data];
		}
		else {
			this.CURRENT_BATCH_DATA.push(data);
		}

		//
	}

	//
}
