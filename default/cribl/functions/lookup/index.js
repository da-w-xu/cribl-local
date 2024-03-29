exports.name = 'Lookup';
exports.version = '0.2';
exports.group = 'Standard';

const { CSV, LookupSpec } = C.internal.Lookup;
const { createHmac } = C.Crypto;
const cLogger = C.util.getLogger('func:lookup');
const ENV_ROLE = 'CRIBL_ROLE';
const ENV_CONFIG_HELPER = 'CONFIG_HELPER';

let table;
let file;
let addToEventFunc;
let defaultValues;
const QUOTE_REGEX = /(\\")/g;
let guid;
let logMultiple;
let logCount;
const maxLogMultiple = 100000;

function quote(str) {
  const newStr = str.replace(QUOTE_REGEX, '\\$1');
  return newStr.length === str.length && !/\s/.test(str) ? str : `"${newStr}"`;
}

function addToRaw(event, fields, values) {
  if (!event._raw || !fields || !values || !fields.length || !values.length) {
    return;
  }
  let delim = event._raw.length > 0 ? ',' : '';
  for (let i = 0; i < fields.length && i < values.length; i++) {
    const v = values[i];
    if (v !== undefined) {
      event._raw += `${delim}${quote(fields[i])}=${quote(v || '')}`;
      delim = ',';
    }
  }
}

exports.init = (opts) => {
  const conf = opts.conf || {};
  file = conf.file;
  table = undefined;
  addToEventFunc = undefined;
  logCount = 0;
  logMultiple = 1;
  const matchMode = conf.matchMode || 'exact';
  const matchType = conf.matchType || 'first';
  const outFields = conf.outFields || [];
  const inEventFields = [];
  const inLookupFields = [];
  const outEventFields = [];
  const outLookupFields = [];
  defaultValues = [];
  return Promise.resolve()
    .then(() => {
      conf.inFields.forEach(inF => {
        inEventFields.push(inF.eventField);
        inLookupFields.push(inF.lookupField);
      });
      defaultValues.fill(undefined, 0, outFields.length);
      for (let i = 0; i < outFields.length; i++) {
        const outF = outFields[i];
        outEventFields.push(outF.eventField);
        outLookupFields.push(outF.lookupField);
        if (outF.defaultValue !== undefined) defaultValues[i] = outF.defaultValue;
      }
      defaultValues = defaultValues.find(x => x != null) != null ? defaultValues : undefined;
      addToEventFunc = conf.addToEvent ? addToRaw : undefined;
      const ignoreCase = conf.ignoreCase || false;
      const reloadPeriodSec = (+conf.reloadPeriodSec) || -1;
      guid = createHmac(file, `${matchMode}${matchType}${ignoreCase}${Date.now()}${process.env['CRIBL_WORKER_ID']||''}`).substring(0,10);
      const ls = new LookupSpec(inEventFields, outLookupFields, inLookupFields, outEventFields, false, ignoreCase);
      // Best effort to create unique ID to identify each function instance.
      cLogger.info('Creating Lookup', { matchMode, matchType, file, guid, confReloadPeriodSec: reloadPeriodSec });
      table = CSV.getReference(file, ls, reloadPeriodSec, matchMode, matchType);
      cLogger.info('Lookup created', { matchMode, matchType, file, guid, tableIsNull: table == null, actualReloadPeriodMs: table.getServicePeriod() });
      return table.ready()
        .catch(err => {
          if (err.code === 'ENOENT' && isConfigHelper()) {
            cLogger.debug(`missing lookup file ${file} in cfg. helper`, { error: err, guid });
            return [{
              func: exports.name,
              severity: 'warn',
              message: `The specified lookup file ${file} couldn't be found in this instance. Make sure it's present in Worker nodes.`
            }];
          }
          throw err;
        });
    });
};

function isConfigHelper() {
  return process.env[ENV_ROLE] === ENV_CONFIG_HELPER;
}

exports.unload = () => {
  if (table) {
    table.release();
    cLogger.info('unloaded lookup function', { path: file, id: table.getId(), remainingRefs: table.getRefCount(), guid });
  } else {
    cLogger.info('unloaded lookup function, no table present', { path: file, guid });
  }
  table = undefined;
};

function logWithBackoff(logFunc, message, args) {
  if ((++logCount % logMultiple) === 0) {
    logFunc(message, args ?? {});
    if (logMultiple < maxLogMultiple) logMultiple *= 2;
  }
}

exports.process = (event) => {
  if (table) {
    // ServiceLoadable may be in the middle of periodically reloading the lookup table
    return table.ready().then(() => {
      table.lookup(event, addToEventFunc, defaultValues);
      return event;
    }).catch(error => {
      logWithBackoff(cLogger.error,'lookup error', { error, file, guid, numErrors: logCount });
      return event;
    });
  } else {
    logWithBackoff(cLogger.warn,'called process() with no table: event is not modified', { file, guid, numErrors: logCount });
  }
  return event;
};

exports.UT_getLookup = () => {
  return process.env.NODE_ENV === 'test' ? table : undefined;
}
