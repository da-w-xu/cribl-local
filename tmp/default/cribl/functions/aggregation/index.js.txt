exports.name = 'Aggregations';
exports.version = '0.2';
exports.disabled = false;
exports.handleSignals = true;
exports.group = 'Standard';
exports.sync = true;
const os = require('os');

const { Expression, NestedPropertyAccessor } = C.expr;

const cLogger = C.util.getLogger('func:aggregation');


const hostname = os.hostname();
const maxMem = os.totalmem();

let passthrough = false;
let aggOpts;
let aggregator;
let fields2add = [];
let flushOnInputClose;

const CTRL_PREFIX = '_ctrl.';
function getAccessor(fieldName) {
  if (fieldName) {
    return new NestedPropertyAccessor(fieldName);
  }
  return fieldName;
}

exports.init = (opts) => {
  const conf = opts.conf || {};
  passthrough = Boolean(conf.passthrough);
  if (!conf.aggregations || !Array.isArray(conf.aggregations)) {
    return;
  }
  // All aggregation-based configurations go below here
  aggOpts = {
    cumulative: conf.cumulative !== undefined ? conf.cumulative : false,
    sufficientStatsOnly: conf.sufficientStatsOnly !== undefined ? conf.sufficientStatsOnly : false,
    hostname,
    flushEventLimit: Math.floor(conf.flushEventLimit || 0),
    timeWindowSeconds: C.util.parseTimeStringToSeconds(conf.timeWindow),
    aggregations: conf.aggregations,
    splitBys: conf.groupbys,
    lagToleranceSeconds: C.util.parseTimeStringToSeconds(conf.lagTolerance),
    idleTimeLimitSeconds: C.util.parseTimeStringToSeconds(conf.idleTimeLimit),
    flushMemLimit: Math.min(maxMem, C.util.parseMemoryStringToBytes(conf.flushMemLimit || `${maxMem}`, err => { throw err; })),
    metricsMode: Boolean(conf.metricsMode),
    prefix: conf.prefix,
    preserveSplitByStructure: conf.preserveGroupBys || false,
    searchAggMode: conf.searchAggMode,
  };
  aggregator = C.internal.Aggregation.aggregationMgr(aggOpts);

  // Init eval
  const add = [];
  (conf.add || []).forEach(field => {
    field.name = (field.name || '').trim();
    const isCtrlField = field.name.startsWith(CTRL_PREFIX);
    add.push(isCtrlField);
    add.push(isCtrlField ? field.name.substr(CTRL_PREFIX.length) : getAccessor(field.name));
    add.push(new Expression(`${field.value}`, { disallowAssign: true }));
  });
  fields2add = add;
  flushOnInputClose = conf.flushOnInputClose != null ? Boolean(conf.flushOnInputClose) : true;

  cLogger.info('initialized aggregation', { aggOpts, fields2add, flushOnInputClose });
};

function shouldForce(signalType) {
  if (flushOnInputClose) return ['close', 'final'].includes(signalType);
  return signalType === 'final';
}

exports.process = (event) => {
  if (!aggregator) {
    return passthrough || event.__signalEvent__ ? event : undefined;
  }
  let flushedEvents;
  // Signals are treated differently, unless the signal is a reset.
  if (event.__signalEvent__ && event.__signalEvent__ !== 'reset') {
    // suppress flush if the event was a close/cancel, coming from the limit function
    if (event.__signalEvent__ === 'cancel') {
      flushedEvents = [];
    } else {
      flushedEvents = aggregator.flush(shouldForce(event.__signalEvent__));
    }
  } else {
    flushedEvents = aggregator.aggregate(event);
  }
  // Execute Eval
  if (fields2add.length) {
    for (let ei = 0; ei < flushedEvents.length; ei++) {
      const flushedEvent = flushedEvents[ei];
      for (let i = 2; i < fields2add.length; i += 3) {
        const key = fields2add[i - 1];
        const val = fields2add[i].evalOn(flushedEvent);
        if (!fields2add[i - 2]) {
          // might need to throw away the result
          if (key) key.set(flushedEvent, val);
        } else {
          flushedEvent.__setCtrlField(key, val);
        }
      }
    }
  }
  // Pass through events in passthrough mode, or pass through signals that are not reset or complete_gen.
  // Those are special and each aggregator decides whether to reset its own state and whether to
  // send its own reset & complete_gen events rather than propagating the incoming ones.
  if (passthrough || (event.__signalEvent__ && event.__signalEvent__ !== 'reset' && event.__signalEvent__ !== 'complete_gen')) {
    flushedEvents.push(event);
  }
  return flushedEvents;
};

exports.unload = () => {
  if (aggregator) {
    aggregator.close();
  }
};

// UTs
exports.getAggOpts = () => aggOpts;
exports.getAggregator = () => aggregator;
