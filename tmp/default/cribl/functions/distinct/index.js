exports.name = 'Distinct';
exports.version = '0.1.4';
exports.disabled = false;
exports.handleSignals = true;
exports.group = C.INTERNAL_FUNCTION_GROUP;
exports.sync = true;

const DEFAULT_MAX_DISTINCT_COMBINATIONS = 10000;
const DEFAULT_MAX_DEPTH = 15;
const { NestedPropertyAccessor } = C.expr;
const criblInternalField = C.internal.criblInternalField;

let logger;                // logger for this function
let maxCombinations;       // used to limit the number of tracked combinations
let maxDepth;              // maximum number of groupBy properties
let groupBy;               // names of known/registered grouping columns (dynamic for '*')
let isFederated;           // indicator that we run federated -> no enveloping of output events
let singleEventProcessor;  // handles a single data event
let distinctCombinations;  // number of (counted) combinations
let root;                  // set or map, for first (or only) grouping column
let knownFields;           // only used for wildcard mode: names of all known properties
let cancelTriggered;       // we reached the maxCombinations and triggered a cancel

exports.init = (opts) => {
  const conf = opts.conf ?? {};

  maxCombinations = conf.maxCombinations ?? DEFAULT_MAX_DISTINCT_COMBINATIONS;
  maxDepth = conf.maxDepth ?? DEFAULT_MAX_DEPTH;
  isFederated = conf.isFederated ?? false;

  // create a unique instance ID of this function for debugging
  let funcInstId = "";
  for (let idLen = 0; idLen < 5; ++idLen) funcInstId += String.fromCodePoint('a'.charCodeAt(0) + Math.floor(Math.random() * 26));
  logger = C.util.getLogger(`func:distinct`, { funcInstId });

  groupBy = conf.groupBy ?? ['*'];
  if (!Array.isArray(groupBy) || groupBy.length === 0 || (groupBy.length === 1 && groupBy[0] === '*')) {
    // will be populated with field names from data when we process events (wildcard mode)
    groupBy = [];
  } else if (groupBy.length > maxDepth) {
    groupBy = groupBy.splice(0, maxDepth);
    logger.error('limiting list of specified groupBy fields', {groupBy});
    throw new Error('distinct function: number of groupBy fields larger than maxDepth configuration value');
  }

  // we are using three independent implementations for building the distinct
  // lists, depending on the number of groupBy properties:
  // 0 - auto-detect means that we extract the property names from the input
  //     data. we build a tree of maps (from root to leaf) from property values.
  // 1 - single property listed in groupBy means that we maintain a simple set
  //     of the property values
  // n - we create a tree of Map instances with Sets as leafs where each of
  //     the groupBy properties (except for last one) describes the path 
  //     through the Map branches.
  switch(groupBy.length) {
    case 0: // wildCardDistinct (don't know fields yet)
      logger.debug('wildcard mode', { isFederated });
      singleEventProcessor = buildWildCardDistinct;
      break;
    case 1: // single groupBy column
      logger.debug('single groupBy mode', { isFederated });
      groupBy = new NestedPropertyAccessor(groupBy[0]);
      singleEventProcessor = buildDistinctSingle;
      break;
    default: // 2 or more known groupBy columns
      groupBy = groupBy.map((fieldName) => new NestedPropertyAccessor(fieldName));
      logger.debug('multiple groupBy mode', { groupBy, isFederated } );
      singleEventProcessor = buildDistinctFixed;
      break;
  }

  reset();
}

exports.unload = () => {
  groupBy = undefined;
  root = undefined;
  knownFields = undefined;
}

exports.process = (event) => {
  if (!event) return event; // quick out for invalid events

  if (event.__signalEvent__) {
    // filter out close events from log (not interesting for distinct)
    logger.debug('signal event received', { event: event.__signalEvent__ });
 
    switch(event.__signalEvent__) {
      case 'reset':
        reset();
        return event;

      case 'close':
        if (isFederated) {
          // flush on close (and start fresh) if we are running federated
          return flush(event, true);
        } else {
          // ignore close on coordinated side
          return event;
        }

      case 'final':
      case 'complete_gen':
      case 'timer':
          return flush(event);

      default:
        // unhandled signal
        return event;
    }
  } 

  // use scenario-specialized implementation to group data events
  if (distinctCombinations < maxCombinations) {
    singleEventProcessor(event);
  } else if (!cancelTriggered) {
    // send a cancel signal to stop processing any further events
    logger.debug('reached maxCombinations, canceling query');
    cancelTriggered = true;
    const signal = event.__clone(false, []);
    signal.__signalEvent__ = 'cancel';
    signal.__setCtrlField('reason', 'distinct'); // for debugging only
    return signal;
  }
}

/**
 * Called to flush all distinct values to output.
 * @param triggerEvent Used as template for output events
 * @param resetOnFlush Resets the content as part of flush
 * @returns An array of CriblEvent instances
 */
function flush(triggerEvent, resetOnFlush = false) {
  let ret = [];

  // do not envelope output into reset/complete_gen if running on federated
  if (!isFederated) {
    const resetEvent = triggerEvent.__clone(false, []);
    resetEvent.__signalEvent__ = 'reset';
    resetEvent.__setCtrlField('distinct', triggerEvent.__signalEvent__ ?? 'preview');    
    ret.push(resetEvent);
  }

  // delegate the transformation
  if (singleEventProcessor === buildWildCardDistinct) {
    flushWildCardDistinct(triggerEvent.__clone(false,[]), ret, root);
  } else if (singleEventProcessor === buildDistinctFixed) {
    flushDistinctFixed(triggerEvent.__clone(false,[]), ret, root);
  } else {
    flushDistinctSingle(triggerEvent, ret);
  }

  // do not envelope output into reset/complete_gen if running on federated
  if (triggerEvent.__signalEvent__ !== 'complete_gen' && !isFederated) {
    const completeGen = triggerEvent.__clone(false, []);
    completeGen.__signalEvent__ = 'complete_gen';
    completeGen.__setCtrlField('distinct',  'preview');    
    ret.push(completeGen);
  }

  ret.push(triggerEvent); // original final/timer/complete_gen event

  if (resetOnFlush) reset(); // asked to start fresh
  return ret;
}

/**
 * Builds the tree of Map instances along the path of (unknown) groupBy properties.
 * Even the leaf nodes are (empty) Map instances.
 * @param {*} event The event to extract the properties from
 */
function buildWildCardDistinct(event) {
  if (groupBy.length < maxDepth) {
    // detect new properties on this event (not yet seen and to be added to groupBy)
    for (const key of Object.keys(event).filter((k) => !criblInternalField(k))) {
      if (!knownFields.has(key)) {
        groupBy.push(key);
        knownFields.add(key);

        if (groupBy.length === maxDepth) {
          logger.info('limiting list of dynamically created groupBy fields', {groupBy});
          break;
        }
      }
    }    
  }

  buildMapPath(event, false); // fixedLength==false -> create map-only path
}

/**
 * Builds a tree of Maps (branches) and Sets as leaf nodes for fixed number
 * of groupBy properties (with more than one groupBy).
 * @param {*} event The event to extract the property values from
 */
function buildDistinctFixed(event) {
  const leaf = buildMapPath(event);
  const groupByVal = mapValue(groupBy[groupBy.length - 1].get(event));
  if (!leaf.has(groupByVal)) {
    ++distinctCombinations;
    leaf.add(groupByVal);
  }
}

/**
 * Simplest form of distinct: single groupBy property, evaluated as Set.
 * @param {*} event The event to extract the single groupBy property from
 */
function buildDistinctSingle(event) {
  root.add(mapValue(groupBy.get(event)));
  distinctCombinations = root.size;
}

/**
 * Converts the tree of Map instances into CriblEvents of distinct combinations.
 * @param {*} templateEvent Used to clone the distinct properties into
 * @param {*} target The target array to add the output events to
 * @param {*} currNode The current tree node (Map)
 * @param {*} groupByIdx Index in the groupBy array
 */
function flushWildCardDistinct(templateEvent, target, currNode, groupByIdx = 0) {
  const groupByName = groupBy[groupByIdx];
  if (groupByIdx < groupBy.length - 1) {
    // still on branch -> traverse toward leaf
    for (const [groupByValue, nextNode] of currNode.entries()) {
      const templateCopy = templateEvent.__clone(false);
      templateCopy[groupByName] = groupByValue;
      flushWildCardDistinct(templateCopy, target, nextNode, groupByIdx + 1);
    }
  } else {
    // on leaf, create an output event per Key of the leaf-map.
    for (const v of currNode.keys()) {
      const dataEvent = templateEvent.__clone(false);
      
      dataEvent[groupByName] = v;
      target.push(dataEvent);
    }
  }
}

/**
 * Converts the tree of Map (branch) and Set (leaf instances) to output events.
 * @param {*} templateEvent The event to clone to produce output events
 * @param {*} target The array to add output to
 * @param {*} currNode Current position within the tree (Map or Set)
 * @param {*} groupByIdx Index into groupBy array 
 */
function flushDistinctFixed(templateEvent, target, currNode, groupByIdx = 0) {
  const npa = groupBy[groupByIdx];

  if (currNode instanceof Map) {
    // branch -> enrich template and recurse
    for (const [groupByValue, nextNode] of currNode.entries()) {
      const templateCopy = templateEvent.__clone(false);
      npa.deepSet(templateCopy, groupByValue);
      flushDistinctFixed(templateCopy, target, nextNode, groupByIdx + 1);
    }
  } else {
    // leaf, add an event per set entry
    for (const v of currNode) {
      const dataEvent = templateEvent.__clone(false);
      npa.deepSet(dataEvent, v);
      target.push(dataEvent);
    }
  }
}

/**
 * Simple conversion of value Set into output events.
 * @param {*} triggerEvent The event to clone to produce output
 * @param {*} target Target array for output events
 */
function flushDistinctSingle(triggerEvent, target) {
  for (const v of root) {
    const dataEvent = triggerEvent.__clone(false, []);
    groupBy.deepSet(dataEvent, v);
    target.push(dataEvent);  
  }
}

/**
 * Helper to create or fetch the tree branch along the groupBy properties.
 * @param {*} event The event, describing the path through the tree
 * @param {*} fixedLength Indicator if we have known number of groupBy or '*' mode
 * @returns The leaf element of the tree
 */
function buildMapPath(event, fixedLength = true) {
  let pathElement = root;

  // traverse along the groupBy to the Set (fixedLength) or leaf Map (*)
  for (let groupByIdx = 0; groupByIdx < groupBy.length - (fixedLength ? 1 : 0); ++groupByIdx) {
    const groupByVal = mapValue(fixedLength ? groupBy[groupByIdx].get(event) : event[groupBy[groupByIdx]]);
    let nextElement = pathElement.get(groupByVal);

    // branch doesn't exist yet, build it
    if (nextElement == null) {
      nextElement = groupByIdx < groupBy.length - 2 ? new Map() : (fixedLength ? new Set(): new Map());
      pathElement.set(groupByVal, nextElement);

      if (!fixedLength && groupByIdx === groupBy.length - 1) {
        ++distinctCombinations;
      }
    }

    pathElement = nextElement;
  }

  return pathElement;
}

/**
 * Helper to convert a value into something that can be used as key into map.
 * @param {*} val The groupBy value that is used as key
 * @returns The mappable value
 */
function mapValue(val) {
  if (val == null) return null; // null/undefined
  if (typeof val === 'object') return JSON.stringify(val);
  return val;
}

/**
 * Called to reset the content of all data structures.
 */
function reset() {
  if (singleEventProcessor === buildWildCardDistinct) {
    // unknown number of groupBy fields
    root = new Map();
    knownFields = new Set();
    groupBy = [];
  } else if (singleEventProcessor === buildDistinctFixed) {
    // 2 or more groupBy properties
    root = new Map();    
  } else {
    // distinct single     
    root = new Set();
  }

  cancelTriggered = false;
  distinctCombinations = 0;
}
