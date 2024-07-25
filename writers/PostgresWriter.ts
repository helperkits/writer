/* * */

import LOGGER from '@helperkits/logger';
import TIMETRACKER from '@helperkits/timer';

/* * */

interface PostgresWriterOptions {
	batch_size?: number
	filter?: object
	upsert?: boolean
}

/* * */

export default class PostgresWriter {
	//

	private CURRENT_BATCH_DATA = [];

	private DB_CLIENT = null;

	private DB_TABLE = null;

	private INSTANCE_NAME = 'Unnamed Instance';

	private MAX_BATCH_SIZE = 3000;

	private SESSION_TIMER = new TIMETRACKER();

	/* * */

	constructor(instanceName: string, collection, options?: PostgresWriterOptions) {
		if (instanceName) this.INSTANCE_NAME = instanceName;
		if (collection) this.DB_CLIENT = collection;
		if (options?.batch_size) this.MAX_BATCH_SIZE = options.batch_size;
	}

	/* * */

	async flush() {
		try {
			//

			const flushTimer = new TIMETRACKER();
			const sssionTimerResult = this.SESSION_TIMER.get();

			if (this.CURRENT_BATCH_DATA.length === 0) return;

			const keys = Object.keys(this.CURRENT_BATCH_DATA[0]);
			const valuesPlaceholder = keys.map((_, i) => `$${i + 1}`).join(',');

			const insertQuery = `
				INSERT INTO ${this.DB_TABLE} (${keys.join(',')})
				VALUES (${valuesPlaceholder})
				ON CONFLICT DO NOTHING;
			`;

			for (let i = 0; i < this.CURRENT_BATCH_DATA.length; i += this.MAX_BATCH_SIZE) {
				const batch = this.CURRENT_BATCH_DATA.slice(i, i + this.MAX_BATCH_SIZE);
				const values = batch.map(item => keys.map(key => item[key]));
				await this.DB_CLIENT.query(insertQuery, values.flat());
			}

			LOGGER.info(`POSTGRESWRITER [${this.INSTANCE_NAME}]: Flush | Length: ${this.CURRENT_BATCH_DATA.length} | DB Table: ${this.DB_TABLE} (session: ${sssionTimerResult}) (flush: ${flushTimer.get()})`);

			this.CURRENT_BATCH_DATA = [];

			//
		}
		catch (error) {
			LOGGER.error(`POSTGRESWRITER [${this.INSTANCE_NAME}]: Error @ flush(): ${error.message}`);
		}
	}

	/* * */

	async write(data, options = {}) {
		// Check if the batch is full
		if (this.CURRENT_BATCH_DATA.length >= this.MAX_BATCH_SIZE) {
			await this.flush();
		}
		// Reset the timer
		if (this.CURRENT_BATCH_DATA.length === 0) {
			this.SESSION_TIMER.reset();
		}
		// Add the data to the batch
		if (Array.isArray(data)) {
			const combinedDataWithOptions = data.map(item => ({ data: item, options: options }));
			this.CURRENT_BATCH_DATA = [...this.CURRENT_BATCH_DATA, ...combinedDataWithOptions];
		}
		else {
			this.CURRENT_BATCH_DATA.push({ data: data, options: options });
		}
		//
	}

	//
}
