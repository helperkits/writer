/* * */

import LOGGER from '@helperkits/logger';
import TIMETRACKER from '@helperkits/timer';

/* * */

interface MongoDbWriterOptions {
	batch_size?: number
	filter?: object
	upsert?: boolean
	write_mode?: 'replace' | 'update'
}

/* * */

export default class MongoDbWriter {
	//

	private CURRENT_BATCH_DATA = [];

	private DB_COLLECTION = null;

	private INSTANCE_NAME = 'Unnamed Instance';

	private MAX_BATCH_SIZE = 3000;

	private SESSION_TIMER = new TIMETRACKER();

	/* * */

	constructor(instanceName: string, collection, options?: MongoDbWriterOptions) {
		if (instanceName) this.INSTANCE_NAME = instanceName;
		if (collection) this.DB_COLLECTION = collection;
		if (options?.batch_size) this.MAX_BATCH_SIZE = options.batch_size;
	}

	/* * */

	async flush() {
		try {
			//

			const flushTimer = new TIMETRACKER();
			const sssionTimerResult = this.SESSION_TIMER.get();

			if (this.CURRENT_BATCH_DATA.length === 0) return;

			const writeOperations = this.CURRENT_BATCH_DATA.map((item) => {
				switch (item.options?.write_mode) {
					default:
					case 'replace':
						return {
							replaceOne: {
								filter: item.options.filter,
								replacement: item.data,
								upsert: item.options?.upsert ? true : false,
							},
						};
					case 'update':
						return {
							updateOne: {
								filter: item.options.filter,
								update: item.data,
								upsert: true,
							},
						};
				}
			});

			await this.DB_COLLECTION.bulkWrite(writeOperations);

			LOGGER.info(`MONGODBWRITER [${this.INSTANCE_NAME}]: Flush | Length: ${this.CURRENT_BATCH_DATA.length} | DB Collection: ${this.DB_COLLECTION.collectionName} (session: ${sssionTimerResult}) (flush: ${flushTimer.get()})`);

			this.CURRENT_BATCH_DATA = [];

			//
		}
		catch (error) {
			LOGGER.error(`MONGODBWRITER [${this.INSTANCE_NAME}]: Error @ flush(): ${error.message}`);
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
}
