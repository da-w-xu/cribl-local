exports.name = 'send';
exports.version = '0.0.2';
exports.disabled = false;
exports.handleSignals = true;
exports.group = C.INTERNAL_FUNCTION_GROUP;

const { randomBytes } = require('crypto');
let url,
  group = 'default',
  workspace = 'main',
  sendUrlTemplate,
  searchId,
  tee = false,
  flushMs = 10000,
  destination,
  lastFlush,
  logger,
  _eventsOut = 0,
  _eventsDropped = 0,
  _bytesOut = 0,
  suppressPreviews = false,
  receivedEvents = false,
  // identify this send function in case of federation.
  sendId = `${Date.now()}${randomBytes(4).toString('hex')}`;

exports.init = async (opt) => {
  const conf = opt.conf;
  ({ url, searchId, sendUrlTemplate } = conf);

  if (!searchId) throw new Error('searchId field is required');
  if (!sendUrlTemplate) throw new Error('sendUrlTemplate field is required');

  logger = C.util.getLogger(`sendFunction:${searchId}`);
  group = conf.group ?? group;
  workspace = conf.workspace ?? workspace;
  sendUrlTemplate = conf.sendUrlTemplate ?? sendUrlTemplate;
  tee = conf.tee ?? tee;
  flushMs = conf.flushMs ?? flushMs;
  suppressPreviews = conf.suppressPreviews;
  if (!url) {
    // if url is not provided construct it from the template
    url = templateStr(sendUrlTemplate, {
      workspace,
      group,
    });
  }

  destination = await C.internal.kusto.send.createDestination({
    url,
    searchId,
  });
  _eventsOut = 0;
  _bytesOut = 0;
  _eventsDropped = 0;
  logger.info('Initialized send', { url, tee, searchId });
};

exports.process = async (event) => {
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
        // ignore limit signals
        if (event.__ctrlFields.includes('cancel')) return event;
        logger.debug('Final flushing stats & destination');
        // flush destination on final to make sure we are clean to shutdown.
        await destination.flush();
        await destination.close();
        const stats = createStatsEvent(event, destination, true);
        stats.status = 'Sending complete';
        return [event, stats];
      }
      default:
        logger.warn('unhandled signal', { event });
        // ignore unhandled signal
        return event;
    }
  }
  return await send(event, destination, tee);
};

exports.unload = () => {
  destination = undefined;
  // clearing these out for testing
  url = undefined;
  receivedEvents= false;
};

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
 * Replace all occurrences of  \<token\> in *template*
 * @param {string} template templated string like this<token>is<replaced>
 * @param {object} tokens key value pairs to replace by
 * @returns the string with the replacement
 */
function templateStr(template, tokens) {
  return template.replace(/<\w+>/g, (match) => {
    const tk = match.slice(1, -1);
    const rv = tokens[tk];
    if (!rv) {
      throw new Error(`Unknown replace token ${tk}`);
    }
    return rv;
  });
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
  const { sentCount, bytesOut: bOut, numDropped } = toFlush.metrics;

  const eventsOut = sentCount - _eventsOut;
  const bytesOut = bOut - _bytesOut;
  const eventsDropped = numDropped - _eventsDropped;
  // nothing to report
  if (!force && bytesOut === 0 && eventsOut === 0 && eventsDropped === 0) return undefined;
  const rv = Object.assign(cloned, {
    sendId,
    url,
    searchId,
    eventsOut,
    totalEventsOut: sentCount,
    bytesOut,
    totalBytesOut: bOut,
    eventsDropped,
    totalEventsDropped: numDropped,
    status: 'Sending',
    _time: Date.now() / 1000,
  });
  _eventsOut = sentCount;
  _bytesOut = bytesOut;
  _eventsDropped = numDropped;
  return rv;
}

/**
 * flush stats if lastFlush was more than flushMs ago.
 * If a signal was passed include it in the returned events.
 * @param {CriblEvent} signalOrEvent
 * @returns array of events or undefined
 */
function flushStats(signalOrEvent) {
  const rv = [];
  if (!receivedEvents) {
    receivedEvents = true;
    const cloned = signalOrEvent.__clone(true, []);
    const startEvt = Object.assign(cloned, {
      searchId,
      sendId,
      url,
      status: 'Begin sending',
      group,
      workspace,
      _time: Date.now() / 1000,
    });
    rv.push(startEvt);
  }
  if (signalOrEvent.__signalEvent__) {
    rv.push(signalOrEvent);
  }
  const now = Date.now();
  // wait at least 1 flushMs to flush
  if (!lastFlush) lastFlush = now;
  if (!suppressPreviews && lastFlush + flushMs < now) {
    logger.debug('timer flushing stats');
    const stats = createStatsEvent(signalOrEvent, destination);
    lastFlush = now;
    stats && rv.push(stats);
  }

  return rv.length ? rv : undefined;
}

exports.UT_getDestination = () => destination;