exports.name = 'Store';
exports.version = '0.0.3';
exports.disabled = false;
exports.handleSignals = true;
exports.group = C.INTERNAL_FUNCTION_GROUP;

const logger = C.util.getLogger('func:store');
const MAX_EVENT_DEFAULT = 10000;
const COMPRESS_AUTO_SIZE = 16384; // compress if payload > 16kb
const NEWLINE = '\n';

// exists to make it possible to override by unit test
let maxEventDefault = MAX_EVENT_DEFAULT;
let lookupRecordsLimit = C.internal.kusto.DEFAULT_SORT_MAX_EVENTS_IN_MEMORY;
let compressAutoSize = COMPRESS_AUTO_SIZE;

let destination;       // i.e., the lookup name
let description;       // stored as description alongside the knowledge object
let fieldMapping;      // optional mapping of event properties to output field names
let overwrite;         // indicator that the output is supposed to be overwritten if existing
let append;            // indicator that the output is supposed to be appended; redundant indicator for convenience
let tee;               // if true, we output the original events, otherwise metrics
let compress;          // "true", "false" or "auto"
let separator = ',';   // separator character, i.e., for CSV files (lookups); hard-coded to comma until support for custom separators is added
let maxEvents;         // maximum number of events to keep in memory
let suppressPreviews;  // indicator that we don't want to flush stats on timer events

let totalEvents;  // actual seen events (might be > max)
let hadFinal;     // indicator that a final signal was received
let outputBuffer; // array of cribl events until we see a final signal

/**
 * Initializes the store pipeline function
 * @param opt The configuration for the storing operation
 */
exports.init = async (opt) => {
  const conf = opt.conf;

  if (conf.type !== 'lookup') {
    // other output types beyond CSV may be added later
    throw new Error(`invalid store type: ${conf.type}`);
  }

  if (!(destination = conf.destination)) {
    throw new Error(`destination required for store type ${conf.type}`);
  }

  // common initialization
  overwrite = conf.mode === 'overwrite';
  append = conf.mode === 'append';
  tee = conf.tee ?? false;
  maxEvents = conf.maxEvents ?? maxEventDefault;
  maxEvents = Math.min(lookupRecordsLimit, maxEvents);
  suppressPreviews = conf.suppressPreviews ?? false;
  // initialize the buffer
  reset();

  // mode specific initialization
  if (append) {
    const lookupMetadata = await C.internal.kusto.store.getLookupContentAndStoredMetadata(destination, true);
    if (!lookupMetadata) {
      // if file doesn't exist, switch to default 'create' mode
      append = false;
    } else if (lookupMetadata.meta.rows >= lookupRecordsLimit) {  // file exists and guardrail limit is reached
      // For some tables this metadata doesn't exist or incorrect (for compressed file).
      // However, having 'rows' metadata computed allows us to throw error early on and avoid loading whole lookup content in memory
      throw new Error(`Lookup table ${destination} is too large to append content to`);
    }
  }

  if (!append) { // in case of append operation, ignore to keep data consistency
    description = conf.description;
    fieldMapping = conf.fieldMapping;
    if (conf.compress != null) {
      if (conf.compress === 'true' || conf.compress === true) {
        compress = 'true';
      } else if (conf.compress === 'false' || conf.compress === false) {
        compress = 'false';
      } else if (conf.compress === 'auto') {
        compress = 'auto';
      } else {
        throw new Error(`Invalid value for compress option: ${compress}`);
      }
    } else {
      compress = 'auto';
    }
  } else {
    const lookupData = await C.internal.kusto.store.getLookupContentAndStoredMetadata(destination, false, lookupRecordsLimit);

    description = lookupData.meta.description;
    compress = lookupData.meta.compressed ? 'true' : 'auto';

    // temporary step until true append is implemented
    // TO-DO: remove as part of https://taktak.atlassian.net/browse/SEARCH-3706
    maxEvents = Math.min(lookupData.content.totalCount + maxEvents, lookupRecordsLimit);

    // lookupData.content.totalCount is used here instead of lookupData.meta.rows because lookupData.meta.rows meta data could be missing for some lookups and more importantly is incorrect for compressed lookup files
    totalEvents += lookupData.content.totalCount;
    if (totalEvents > maxEvents) {
      throw new Error(`Lookup table ${destination} is too large to append content to`);
    }

    // when appending, we need to keep order of lookup fields as is. As a result no sorting is performed on fields array here
    fieldMapping = {};
    lookupData.content.fields.forEach((fieldName) => {
      if (fieldName === '__id') return;  // skip lookup-specific internal field
      fieldMapping[fieldName] = fieldName;
    });

    // format lookup data, from array of arrays to array of objects
    lookupData.content.items.map((row) => {
      let synEvent = Object.create(null);  // optimized way to mimic events
      for (let i = 0; i < row.length; ++i) {
        if (lookupData.content.fields[i] === '__id') continue;  // skip lookup-specific internal field
        synEvent[lookupData.content.fields[i]] = row[i];
      }
      outputBuffer.push(synEvent);
    });
  }
}

/**
 * Processes a single input event
 * @param event The cribl event to process
 * @returns Depending on tee-mode, original event or null
 */
exports.process = async (event) => {
  if (!event || (hadFinal && event.__signalEvent__ !== 'reset')) return event; // quick out for invalid events

  // handle signals first
  if (event.__signalEvent__) {
    switch (event.__signalEvent__) {
      case 'reset':
        logger.debug('resetting output to store');
        reset();

      case 'timer':
        if (suppressPreviews) return event;
        return [createStatsEvent(event), event];

      case 'final':
        // skip the finals that are caused by limit/take
        if (event.__ctrlFields.includes('cancel')) return event;
        logger.debug('result generation triggered by final event', { event: event });
        hadFinal = true;

        // right now, we only support lookups -> therefore store as CSV
        const storeStats = await storeCSV(event);
        if (!tee) {
          // tee is off, so we return basic stats on final
          return [storeStats, event];
        }

      default:
        logger.silly('ignored signal event', { event: event });
    }

    return event;
  }

  if (append) {
    // filter fields out
    const keys = Object.keys(fieldMapping);
    for (let key in event) {
      if (!keys.includes(key)) delete event[key];
    };
  }

  // data events -> add to buffer until full
  ++totalEvents;

  if (outputBuffer.length < maxEvents) {
    outputBuffer.push(event);
  }

  if (tee) {
    return event;
  }

  // tee is off, so consume that event
  return null;
}

/**
 * Clean up on UnLoad.
 */
exports.unload = () => {
  logger.debug('unloading store function');
  fieldMapping = undefined;
  outputBuffer = undefined;
};

/**
 * Resets (or initializes) the buffer for input events.
 */
function reset() {
  outputBuffer = [];
  hadFinal = false;
  totalEvents = 0;
}

/**
 * Helper to derive field name mappings from input data.
 * This function is essentially a NoOp if the field mapping was provided as part
 * of the pipeline function configuration. Otherwise, it iterates all events to 
 * create a set of all distinct property names and uses these as known fields.
 */
function ensureFieldMapping() {
  if (fieldMapping) return;
  const nameSet = new Set();
  for (const evt of outputBuffer) {
    for (const k of Object.keys(evt)) {
      if (!C.internal.criblInternalField(k)) {
        nameSet.add(k);
      }
    }
  }

  fieldMapping = {};
  Array.from(nameSet).sort().forEach((fieldName) => fieldMapping[fieldName] = fieldName);
  logger.debug('determined field mapping for lookup output', fieldMapping);
}

/**
 * Helper to produce basic metrics/stats event based on seen event numbers.
 * @param evt a CriblEvent to be cloned as base of the stats event
 * @returns A CriblEvent with basic statistics
 */
function createStatsEvent(evt) {
  const ret = evt.__clone(true, [])
  const totalEventsOut = outputBuffer.length;

  ret._time = Date.now() / 1000;
  ret.totalEventsOut = totalEventsOut;
  ret.totalEventsDropped = totalEvents - totalEventsOut;

  return ret;
}

/**
 * Converts the input events to CSV and stores it as lookup file.
 * Right now, we only support storing lookup files from within this pipeline
 * function and lookup files are stores as CSV. This method converts the buffer
 * of cribl events into one large CSV string and delegates the persisting of the
 * data to a utility helper.
 * 
 * @param evt a CriblEvent to be cloned as base of the stats event
 * @returns A CriblEvent with basic statistics
 */
async function storeCSV(evt) {
  // helper to escape separator characters and quotes in CSV data (and title)
  const escapeTestRegex = new RegExp(`"|(${separator})`);
  const escapeQuoteRegex = new RegExp('"', 'g');
  const csvEscape = (str) => {
    if (!escapeTestRegex.test(str)) return str;
    return `"${str.replace(escapeQuoteRegex, '""')}"`;
  }

  // make sure that we have a property name -> CSV title mapping
  ensureFieldMapping();

  // generate the header row
  const keys = Object.keys(fieldMapping);
  if (keys.length === 0) {
    throw new Error(`Can't export to empty ${destination} lookup table with no lookup fields defined`);
  }
  const header = keys.map((k) => csvEscape(fieldMapping[k])).join(separator);

  // generate all CSV data rows
  const dataRows = outputBuffer.map((event) =>
    keys.map((k) => {
      const v = event[k];
      if (v == null) return '';
      return csvEscape(String(v));
    }).join(separator)
  ).join(NEWLINE);

  // wrap the result in a ILookupFile instance
  const content = [header, dataRows].join(NEWLINE);
  let compressOut = compress === 'true' || (compress === 'auto' && content.length >= compressAutoSize);
  // Future enhancement: in case of append operation, implement the following: if content is comprised of many compressed chunks, compress into one large chunk or larger chunks such that each chunk can fit into memory.
  // this work would require adding chunk-related metadata, e.g. list of chunks and for each chunk, start and end positions of chunks 


  const lookupFile = {
    id: `${destination}.csv${compressOut ? '.gz' : ''}`,
    description,
    size: content.length,
    content,
    rows: outputBuffer.length,
  }

  // delegate the storing of the CSV content 
  await C.internal.kusto.store.writeLookupFile(lookupFile, overwrite || append);
  logger.debug('stored CSV content to lookup', { id: lookupFile.id, size: lookupFile.size, rows: lookupFile.rows });

  // since currently it's not a true append under the hood, i.e. outputBuffer contains both currently stored records and incoming records, simply compress outputBuffer 
  // once we move to true append implementation, in autocompress mode perform a follow-up step to compress whole file if certain condition is met like too many compressed chunks or rows
  const stats = createStatsEvent(evt);
  stats.totalBytesOut = lookupFile.size;
  stats.lookupFile = lookupFile.id;
  outputBuffer = []; // no longer needed
  return stats;
}

// any better approach to override these values in unit testing?
exports.UT_setMaxEventDefault = (val) => {
  maxEventDefault = val;
}

exports.UT_setLookupRecordsLimit = (val) => {
  lookupRecordsLimit = val;
}

exports.UT_setAutoCompressSize = (val) => {
  compressAutoSize = val;
}

exports.UT_getMaxEventDefault = () => {
  return maxEventDefault;
}

exports.UT_getLookupRecordsLimit = () => {
  return lookupRecordsLimit;
}

exports.UT_getAutoCompressSize = () => {
  return compressAutoSize;
}