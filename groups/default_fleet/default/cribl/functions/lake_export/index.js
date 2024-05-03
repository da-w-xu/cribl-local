exports.name = 'lake_export';
exports.version = '0.0.1';
exports.disabled = false;
exports.handleSignals = true;
exports.group = C.INTERNAL_FUNCTION_GROUP;

let dataset,
  searchJobId,
  tee = false,
  flushMs = 10000,
  destination,
  destMeta,
  lastFlush,
  logger,
  _eventsOut = 0,
  _deltaEventsMissingFields = 0,
  _eventsMissingFields = 0,
  _eventsSkippedFieldPartitioning = 0,
  _bytesOut = 0,
  countEventsWithMissingFields = false,
  suppressPreviews = false,
  receivedEvents = false,
  hadFinal = false, // indicator that a final signal was received
  mock;

exports.init = async (opt) => {
  const conf = opt.conf;
  ({ dataset, searchJobId, mock } = conf);

  if (dataset == null) {
    if (!searchJobId) throw new Error('searchJobId field is required');
  }

  logger = C.util.getLogger(`lakeExportFunction:${searchJobId}`);
  suppressPreviews = conf.suppressPreviews;
  tee = conf.tee ?? tee;
  flushMs = conf.flushMs ?? flushMs;
  const output = await C.internal.kusto.lake.createLakeOutput(dataset, searchJobId, logger, mock);
  if (output == null || output.destination == null) {
    throw new Error(`Couldn't export results to lake dataset '${dataset}'`);
  }
  destination = output.destination;
  destMeta = output.destMeta;
  
  countEventsWithMissingFields = destMeta && destMeta.acceleratedFields && destMeta.acceleratedFields.length > 0;
  _eventsOut = 0;
  _bytesOut = 0;
  _eventsMissingFields = 0;
  _eventsSkippedFieldPartitioning = 0;
  logger.info('Initialized lake export', { dataset });
};

exports.process = async (event) => {
  if (!event || hadFinal) return event; // quick out for invalid events

  if (!tee && event.__signalEvent__) {
    switch (event.__signalEvent__) {
      case 'reset':
      case 'complete_gen':
        // swallow aggregation signals
        // when tee == false, to prevent them from
        // removing re-writing the output
        return;
      case 'timer':
        return flushStats(event);
      case 'final': {
        await flushDest(event);

        const stats = createStatsEvent(event, destination, true);
        stats.status =  'Exporting complete';
        const returnedEvents = [event];
        if (_eventsMissingFields > 0) {
          const warnEvent = Object.assign(event.__clone(true, []), {
            searchJobId,
            status: `Warning: ${_eventsMissingFields} events are missing accelerated fields - you should define the fields for performance purposes. Please refer to https://docs.cribl.io/lake/datasets/#accelerated-fields for more details on accelerated fields.`,
            _time: Date.now() / 1000,
          });
          returnedEvents.push(warnEvent);
        }
        if (_eventsSkippedFieldPartitioning > 0) {
          const warnEvent = Object.assign(event.__clone(true, []), {
            searchJobId,
            status: `Warning: Could not accelerate fields for ${_eventsSkippedFieldPartitioning} events - string length was too long. Please refer to https://docs.cribl.io/lake/datasets/#accelerated-fields for more details on accelerated fields.`,
            _time: Date.now() / 1000,
          });
          logger.warn('Export to lake skipped field acceleration', {_eventsSkippedFieldPartitioning});
          returnedEvents.push(warnEvent);
        }
        returnedEvents.push(stats);
        return returnedEvents;
      }
      default:
        logger.warn('unhandled signal', { event });
        // ignore unhandled signal
        return event;
    }
  } else if (event.__signalEvent__ === 'final') {
    // flush & close on 'final' signal when tee=true
    await flushDest(event);
  }

  if (countEventsWithMissingFields && !event.__signalEvent__) {
    // don't count events with missing fields for non signals
    for (const field of destMeta.acceleratedFields) {
      if (!Object.hasOwn(event, field)) {
        _deltaEventsMissingFields++;
        break;
      }
    }
  }

  return await send(event, destination, tee);
};

exports.unload = () => {
  destination = undefined;
  // clearing these out for testing
  dataset = undefined;
  receivedEvents = false;
  hadFinal = false;
};

async function flushDest(event) {
  // ignore limit signals
  if (event.__ctrlFields.includes('cancel')) return event;
  logger.debug('Final flushing stats & destination');
  hadFinal = true;
  // flush destination on final to make sure we are clean to shutdown.
  await destination.flush();
  await destination.close();
}

/**
 * Send events through the destination.
 * if tee is true we clone the event bf sending them and return the original if false we send the event and return undefined or stats if due.
 * @param {CriblEvent} evt the event
 * @param {Outputter} destination destination
 * @param {boolean} tee
 * @returns events
 */
async function send(evt, destination, tee = false) {
  if (tee) {
    const cloned = evt.__clone();
    await destination.send(cloned);
    return evt;
  }
  await destination.send(evt);
  return flushStats(evt);
}

/**
 * Create a stats event from a cribl event with the status from the destination,
 * if there's no change on the nothing will be reported unless force == true
 * @param {CriblEvent} evt
 * @param {Outputter} destination
 * @param {boolean} force
 * @returns CriblEvent
 */
function createStatsEvent(evt, destination, force = false) {
  const toFlush = destination.reportStatus();
  const cloned = evt.__clone(true, []);
  const { sentCount: totalEventsOut, bytesWritten: totalBytesOut, numEventsSkippedFieldPartition } = toFlush.metrics;

  const deltaEventsOut = totalEventsOut - _eventsOut;
  const deltaBytesOut = totalBytesOut - _bytesOut;
  if (countEventsWithMissingFields) {
    _eventsMissingFields += _deltaEventsMissingFields;
  }
  _eventsSkippedFieldPartitioning = numEventsSkippedFieldPartition;

  // nothing to report
  if (!force && deltaBytesOut === 0 && deltaEventsOut === 0) return undefined;
  const rv = Object.assign(cloned, {
    searchJobId,
    eventsOut: deltaEventsOut,
    bytesOut: deltaBytesOut,
    totalEventsOut,
    totalBytesOut,
    status: 'Exporting',
    _time: Date.now() / 1000,
  });
  _eventsOut = totalEventsOut;
  _bytesOut = totalBytesOut;
  return rv;
}

/**
 * flush stats if lastFlush was more than flushMs ago.
 * If a signal was passed include it in the returned events.
 * @param {CriblEvent} signalOrEvent
 * @returns array of events or undefined
 */
function flushStats(signalOrEvent) {
  const now = Date.now();
  if (!receivedEvents) {
    const rv = [];
    receivedEvents = true;
    const cloned = signalOrEvent.__clone(true, []);
    const startEvt = Object.assign(cloned, {
      searchJobId,
      status: 'Begin exporting',
      _time: Date.now() / 1000,
    });
    rv.push(startEvt);
    if (signalOrEvent.__signalEvent__) {
      rv.push(signalOrEvent);
    }
    return rv;
  }

  // wait at least 1 flushMs to flush
  if (!lastFlush) lastFlush = now;
  if (suppressPreviews || lastFlush + flushMs >= now) {
    if (signalOrEvent.__signalEvent__) {
      return signalOrEvent;
    }
    return undefined;
  } else {
    const rv = [];
    if (signalOrEvent.__signalEvent__) {
      rv.push(signalOrEvent);
    }
    const stats = createStatsEvent(signalOrEvent, destination);
    lastFlush = now;
    stats && rv.push(stats);
    return rv.length ? rv : undefined;
  }
}

exports.UT_getDestination = () => destination;
