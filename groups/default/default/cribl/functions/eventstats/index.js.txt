exports.name = 'EventStats';
exports.version = '0.1.0';
exports.disabled = false;
exports.handleSignals = true;
exports.group = C.INTERNAL_FUNCTION_GROUP;
exports.sync = true;

const os = require('os');
const AGG_NAME = /as\(([^)]+)\)$/;
const DEFAULT_MAX_EVENTS = 50000;
const logger = C.util.getLogger('func:eventstats');

let aggOpts;        // aggregation options to create new aggregator
let eventStore;     // captures all raw events (to be enriched with aggregations)
let maxEvents;      // maximum number of events in memory (from conf)
let aggregator;     // instance created in init
let aggregateNames; // found names of the aggregates
let groupBys;       // defined groupBy fields (array of strings)
let hadFinal;       // indicator that we received the final signal
let hitLimit;       // indicator that we do ignore new input data because maxEvents was reached

/**
 * Initializes the EventStats pipeline function.
 * The EventStats pipeline function is very similar to aggregations with the 
 * key difference that it doesn't replace the input with the aggregation events
 * but injects the aggregates into each input event upon flush. This allows for
 * comparisons of event-local values with a rolled-up aggregate. Just like
 * standard aggregates, it does support groupBy columns.
 * 
 * @param {*} opts pipeline configuration object
 */
exports.init = (opts) => {
  const conf = opts.conf ?? {};
  if (!Array.isArray(conf.aggregations) || conf.aggregations.length === 0) {
    logger.warn('empty aggregation list in configuration', conf);
    return;
  }

  // extract the aggregation names
  aggregateNames = [];
  for (let aggregateIdx = 0; aggregateIdx < conf.aggregations.length; ++aggregateIdx) {
    const aggregateExpression = conf.aggregations[aggregateIdx];
    const nameMatch = aggregateExpression.match(AGG_NAME);

    if (nameMatch?.length !== 2) {
      logger.warn('invalid aggregation expression', { conf, aggregateExpression});
      throw new Error('failed to extract name out of aggregation expression');
    }

    aggregateNames.push(nameMatch[1]);
  }

  maxEvents = conf.maxEvents != null ? conf.maxEvents : DEFAULT_MAX_EVENTS;
  if (maxEvents < 1 || !Number.isInteger(maxEvents)) {
    logger.error('invalid value for maxEvents', { conf });
    throw new Error('maxEvents not specified within valid range (or not an integer');
  }

  // groupBy columns are optional
  if (conf.groupBys?.length) {
    groupBys = conf.groupBys;
  }

  // aggOpts will be reused upon resets to create new aggregators
  aggOpts = {
    cumulative: true,
    sufficientStatsOnly: false,
    hostname: os.hostname(),
    flushEventLimit: 0,
    timeWindowSeconds: Number.MAX_SAFE_INTEGER,
    aggregations: conf.aggregations,
    splitBys: groupBys,
    flushMemLimit: os.totalmem(),
    metricsMode: false,
    preserveSplitByStructure: true,
    searchAggMode: 'CoordinatedSuppressPreview',
  };

  reset();
  logger.debug('initialized function', { conf, aggregateNames });
}

/**
 * Dereferences all data, kept by the pipeline function.
 */
exports.unload = () => {
  aggOpts = undefined;
  eventStore = undefined;
  aggregator = undefined;
  aggregateNames = undefined;
  groupBys = undefined;
}

/**
 * Processes a single input event.
 * The input event can either be data (to be stored and aggregated) or signal
 * events, triggering the merge of aggregations with the stored input data.
 * 
 * @param {*} event The data event ot signal to process
 * @returns Array of data events, the original signal event or undefined
 */
exports.process = (event) => {
  if (!aggregator || !event || (hadFinal && event.__signalEvent__ !== 'reset')) return event; // quick out for invalid events

  if (event.__signalEvent__) {
    switch(event.__signalEvent__) {
      case 'reset':
        logger.debug('received reset signal');
        reset();
        return event;

      case 'final':
        if (event.__ctrlFields?.includes('cancel') || hadFinal) return event;
        hadFinal = true;

      // create a preview (complete_gen) or fall-through from 'final'
      case 'complete_gen':
        return flushStats(aggregator.flush(true), event);

      default:
        // unhandled signal
        return event;                
    }
  } else {
    if (eventStore.length >= maxEvents && !hitLimit) {
      logger.warn('exceeded the maximum, configured capacity', { maxEvents });
      hitLimit = true;
    }

    // not a signal event
    if (!hitLimit) eventStore.push(event);
    const flushedAggregates = aggregator.aggregate(event);
    if (!flushedAggregates?.length) return undefined;
    return flushStats(flushedAggregates, event);
  }
}

/**
 * Produces the output in which aggregates are merged into the stored source data.
 * This function is called when the final signal event is received or a preview
 * is requested. It merges the aggregates into the source data, based on the 
 * groupBy columns.
 * 
 * @param {*} aggregates aggregator output 
 * @param {*} triggerEvent the event that triggered the flush
 * @returns the source events, enriched by the aggregates
 */
function flushStats(aggregates, triggerEvent) {
  if (!aggregates?.length || !eventStore.length) return undefined;
  let aggregatedStats;
  let source;

  if (triggerEvent.__signalEvent__) {
    // triggered by signal, we can use the original data as output
    source = eventStore;
    eventStore = [];
  } else {
    // we (aggregator) decided to produce output (keep the original data)
    source = [...eventStore];
  }

  if (groupBys) {
    aggregatedStats = new Map();

    // create a hierarchy Map<string,Map|CriblEvent> along the groupBy columns
    for (let aggregateIdx = 0; aggregateIdx < aggregates.length; ++aggregateIdx) {
      const currAggregate = aggregates[aggregateIdx];
      if (currAggregate.__signalEvent__) continue;

      let targetGroup = aggregatedStats;
      for (let groupByIdx = 0; groupByIdx < groupBys.length; ++groupByIdx) {
        const groupByValue = String(currAggregate[groupBys[groupByIdx]]);
        let currEntry = targetGroup.get(groupByValue);

        // is this the end of the groupBy path?
        if (groupByIdx + 1 === groupBys.length) {
          if (currEntry != null) {
            logger.warn('multiple aggregates found for same groupBy combination', {aggregates});
            throw new Error('failed to group aggregates correctly');
          }

          // store the aggregate event as value for the last groupByValue
          targetGroup.set(groupByValue, currAggregate);          
        } else {
          // more groupBy qualifiers to come
          if (currEntry == null) {
            targetGroup.set(groupByValue, currEntry = new Map());
          }
          
          targetGroup = currEntry;
        }
      }
    }
  } else {
    // single event (no groupBy) stored as aggregatedStats
    const singleAggregate = aggregates.filter((agg) => agg.__signalEvent__ == null);
    if (singleAggregate.length !== 1) {
      logger.warn('invalid/unexpected aggregator output (w/o groupBy)', {aggregates});
      throw new Error('failed to produce aggregates');
    }

    aggregatedStats = singleAggregate[0];
  }

  // merge in the aggregates into the source events
  for (let srcIdx = 0; srcIdx < source.length; ++srcIdx) {
    const sourceEvent = source[srcIdx];
    const aggregate = groupBys ? matchAggregate(sourceEvent, aggregatedStats) : aggregatedStats;

    if (aggregate) {
      aggregateNames.forEach((aggName) => sourceEvent[aggName] = aggregate[aggName]);
    }
  }

  // wrap the data into reset and complete_gen|final
  const resetEvent = triggerEvent.__clone(false, []);
  resetEvent.__signalEvent__ = 'reset';
  resetEvent.__setCtrlField('eventstats', triggerEvent.__signalEvent__ ?? 'preview');    
  source.unshift(resetEvent);

  if (triggerEvent.__signalEvent__) {
    source.push(triggerEvent);
  } else {
    const completeGen = triggerEvent.__clone(false, []);
    completeGen.__signalEvent__ = 'complete_gen';
    completeGen.__setCtrlField('eventstats',  'preview');    
    source.push(completeGen);  
  }

  return source;
}

/**
 * Helper to find the right aggregation for the provided source event.
 * The function searches for the right aggregation based on the groupBy
 * column values of the source event.
 * @param {*} sourceEvent The event that should be enriched
 * @param {*} groupedAggregates The (restructured) aggregates
 * @returns the right aggregate event, matching the source
 */
function matchAggregate(sourceEvent, groupedAggregates) {
  let sourceAggregateContainer = groupedAggregates;
  for (let groupByIdx = 0; groupByIdx < groupBys.length; ++groupByIdx) {
    const sourceGroupByValue = String(sourceEvent[groupBys[groupByIdx]]);
    const groupByStep = sourceAggregateContainer.get(sourceGroupByValue);

    if (!groupByStep) {
      logger.info('missing aggregate in groupBy hierarchy', {sourceEvent});
      return undefined;
    }

    if (groupByIdx + 1 == groupBys.length) {
      return groupByStep;
    } 

    sourceAggregateContainer = groupByStep;
  }
}

/**
 * Helper to reset running state.
 * Removes all source events and creates a fresh aggregator.
 */
function reset() {
  aggregator = C.internal.Aggregation.aggregationMgr(aggOpts); 
  eventStore = [];
  hadFinal = false;
  hitLimit = false;
}
