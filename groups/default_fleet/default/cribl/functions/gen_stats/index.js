exports.name = 'gen_stats';
exports.version = '0.0.1';
exports.disabled = false;
exports.handleSignals = true;
exports.group = C.INTERNAL_FUNCTION_GROUP;
exports.sync = true;

/**
 * Gen stats function
 * This function is used by .genStats
 * command to generate stats over the objects of
 * a set of datasets.
 *
 * Upon starting & finishing a task the following signals are emitted
 *
 * object_stats/start:
 *
 * Emitted when an object starts streaming.
 * object_stats start
 *  {
 *  // field to index
 *   fields: IStatsField[]
 *   source: object name
 *  }
 *
 * The fields to index are emitted per object because .genStats command
 * can refer to multiple datasets, in the future we expect to extract
 * information about what fields to index from datatypes, so this can vary
 * from task to task, and executors process multiple task in parallel.
 *
 * emitted when an object ends streaming.
 * object_stats/end:
 *  {
 *  source: object name
 * }
 *
 */

/**
 * Map of objectPath -> StatsRecord
 * {
 *  fileInfo,
 *  producer
 *  }.
 */
let statsMap;
/**
 * logger
 */
let logger;
let warned;
const createProducer = C.internal.kusto.createStatsProducer;
exports.init = () => {
  logger = C.util.getLogger(`fn:genStats`);
  statsMap = new Map();
  warned = new Set();
};

exports.UT_getMap = () => {
  return statsMap;
};

exports.UT_getLogger = () => {
  return logger;
};

function validateStart(evt) {
  if (!evt.fileInfo || !evt.fileInfo.name) return 'Missing fileInfo';
  if (!evt.fields || !evt.fields.length) return 'Missing fields';
  if (!evt.source || evt.source === '') return 'Missing source';
  if (!evt.datasetId || evt.datasetId === '') return 'Missing datasetId';
}

exports.process = (evt) => {
  switch (evt.__signalEvent__) {
    case 'object_stats': {
      if (evt.type === 'start') {
        const error = validateStart(evt);
        if (error) {
          logger.error('Invalid object_stats/start event', { error });
          return;
        }
        //initialize producer for object on start event with the fields to collect
        statsMap.set(evt.source, {
          fileInfo: evt.fileInfo,
          producer: createProducer(evt.fields),
          datasetId: evt.datasetId,
        });
        logger.debug('Collecting stats for', {fileInfo: evt.fileInfo, fields: evt.fields});
        return;
      } else if (evt.type === 'end') {
        // on end flush the stats for the object that finished streaming.
        const stats = statsMap.get(evt.source);
        const rv = flushStats(stats, evt);
        statsMap.delete(evt.source);
        logger.debug('Finished collecting stats for', {fileInfo: stats.fileInfo});
        return rv;
      } else {
        logger.warn('Unknown signal type', { event: evt });
        return;
      }
    }
    case 'final': {
      // on final signal flush any remaining stats
      const rv = createMessageCarrier(evt);
      for (const [source, stats] of statsMap.entries()) {
        rv.fileInfos.push(createFileInfoDetail(source, stats));
      }
      statsMap = new Map();
      const events = [evt];
      if(rv.fileInfos.length){
        events.unshift(rv)
      }
      return events;
    }
    case undefined: {
      // not a signal.
      const { source } = evt;
      const producer = statsMap.get(source)?.producer;
      if (!producer) {
        if (warned.has(source)) return;
        // ignore objects we don't know about since we don't know what to collect
        logger.warn("can't find producer for source, ignoring events", { source });
        // warn only once.
        warned.add(source);
        return;
      }
      producer.process(evt);
      // TODO: split stats object by size with start offset
      return;
    }
    default:
      //  passthrough signals
      return evt;
  }
};

/**
 * Creates  Control message for search coordinator
 * That containes the FileInfo's with stats.
 * @param {*} evt
 * @returns ControlMessage[object_stats]
 */
function createMessageCarrier(evt) {
  const rv = evt.__clone(true, []);
  return Object.assign(rv, {
    _messageType: '__control__',
    _messageSubType: 'object_stats',
    fileInfos: [],
  });
}

/**
 * Flush the stats for an object.
 * This function is used on object_stats/end to flush the objects stats.
 * @param {ObjectStats} stats
 * @param {CriblEvent} evt
 * @returns Object stats event
 */
function flushStats(stats, evt) {
  const rv = createMessageCarrier(evt);
  rv.fileInfos.push(createFileInfoDetail(evt.source, stats));
  return rv;
}
/**
 * Creates an object that contains the fully qualified path + the fileInfo object with the stats + the datasetId
 * @param {string} source
 * @param {StatsInfo} stats
 * @returns
 */
function createFileInfoDetail(source, stats) {
  stats.fileInfo.splitStats = [stats.producer.splitInfo];
  return {
    source,
    datasetId: stats.datasetId,
    fileInfo: stats.fileInfo,
  };
}
