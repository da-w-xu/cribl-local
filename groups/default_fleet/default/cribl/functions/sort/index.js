exports.name = 'Sort';
exports.version = '0.2';
exports.disabled = false;
exports.handleSignals = true;
exports.group = C.INTERNAL_FUNCTION_GROUP;

const { Expression } = C.expr;
const logger = C.util.getLogger('func:sort');
const DEFAULT_SORT_MAX_EVENTS_IN_MEMORY = C.internal.kusto.DEFAULT_SORT_MAX_EVENTS_IN_MEMORY;
const SORT_FLUSH_INTERVAL = 1000;

let sortProvider;         // used as sort engine for all events
let hadFinal;             // indicator that we have seen the final event (and can skip everything else)
let externalFlushTrigger; // if true, we have some indication that somebody else causes flushes (aggregation)
let checkFlush;           // true if it is time to flush a preview (every SORT_FLUSH_INTERVAL ms)
let lastFlush;            // time of last flush
let hadDataSinceFlush;    // indicator that at least one data event was added since last preview
let totalDataEvents;      // number of total (sorted) data events (since start or last reset)
let lastPreviewEvents;    // number of events since last preview
let previewCount;         // number of intermediate previews (for diag message)
let timeToFinal;          // time from the first to the last event (for diag message)
let suppressPreviews;            // disable previews, if this flag is true we only emit complete results no intermediate results

/**
 * Initialize the sort provider, used by this pipeline function.
 * @param {*} opts The information for the {@link SortConfig}.
 */
exports.init = (opts) => {
  const conf = opts.conf;

  if (conf.comparisonExpression) {
    const expr = new Expression(conf.comparisonExpression, { disallowAssign: true });
    const compFunc = (left, right) => {
      try {
        return expr.evalOn({ left: left, right: right });
      } catch (err) {
        logger.warn('error during sort comparison', { error: err, left: left, right: right });
        return 0;
      }
    };

    if (!(conf.topN < DEFAULT_SORT_MAX_EVENTS_IN_MEMORY || conf.maxEvents < DEFAULT_SORT_MAX_EVENTS_IN_MEMORY)) {
      // we'll enable MergeSort in future version (allowing for larger sorts)
      conf.topN = DEFAULT_SORT_MAX_EVENTS_IN_MEMORY;
      logger.warn('applying implicit topN to sort', { conf: conf });
    }

    const sortConf = {
      id: conf.sortId ?? `sort-${Date.now()}`,
      compareFunction: compFunc,
    };

    if (conf.topN) sortConf.topN = conf.topN;
    if (conf.maxEvents) sortConf.maxEvents = conf.maxEvents;

    sortProvider = C.internal.kusto.createSort(sortConf);
    hadFinal = false;
    checkFlush = true;
    externalFlushTrigger = false;
    hadDataSinceFlush = false;
    totalDataEvents = 0;
    lastPreviewEvents = 0;
    lastFlush = 0;
    previewCount = 0;
    suppressPreviews = conf.suppressPreviews;
    logger.debug('created sort provider', { id: sortProvider.conf.id, type: sortProvider.type });
  } else {
    throw new Error('No comparison expression on sort configuration');
  }
};

/**
 * Processes a single input event, as source for a sort.
 * @param {*} event The event to add to the sort provider
 * @returns The source provider output or null
 */
exports.process = async (event) => {
  if (!event || (hadFinal && event.__signalEvent__ !== 'reset')) return event; // quick out for invalid events

  if (event.__signalEvent__) {
    switch (event.__signalEvent__) {
      case 'reset':
        logger.debug('resetting sort storage', { id: sortProvider.conf.id, event: event });
        sortProvider.reset();
        hadFinal = false;
        totalDataEvents = 0;
        useExternalFlushTrigger(event);
        return event;

      case 'complete_gen':
        logger.debug('preview generation triggered by complete event', { event: event });
        useExternalFlushTrigger(event);
        const preview = flushPreview(event);
        if (preview?.length) {
          return preview;
        }

        return event; // else case -> no preview data returned

      case 'final':
        // skip the finals that are caused by limit/take
        if (event.__ctrlFields.includes('cancel')) return event;
        logger.debug('result generation triggered by final event', { event: event });
        const drained = [];
        hadFinal = true;

        // we only have to produce the final result of there has been any new data since the
        // last flush of a preview or if the total number of sorted events is indeed larger
        // than the previews. otherwise, the last preview represents the final result already
        if (hadDataSinceFlush || totalDataEvents > lastPreviewEvents) {
          // future version will return the drainable (the generator) directly back
          // into the pipeline. for now, supporting only small sort results, we drain
          // the sort output into an array here.
          for await (const e of sortProvider.drain()) {
            drained.push(e);
          }

          // we didn't have anything in the sort output, quick return here
          if (!drained.length) return event;

          const resetEvent = event.__clone(false, []);
          resetEvent.__signalEvent__ = 'reset';
          resetEvent.__setCtrlField('sort', 'final'); // mark this reset source for debugging

          // return [reset, <...sorted data...>, final]
          drained.unshift(resetEvent);
        } else {
          logger.debug('skipping result generation (no new data since last preview)');
        }

        drained.push(event); // the final signal
        timeToFinal = Date.now() - timeToFinal;
        logger.debug('done with sort (after final event)', {
          id: sortProvider.conf.id,
          stats: sortProvider.sortStats,
          previews: previewCount,
          totalTime: timeToFinal,
        });

        return drained;

      default:
        logger.silly('ignored signal event', { event: event });
        return event; // unhandled signal event
    }
  }

  // not a signal event, add it to the sort engine
  hadDataSinceFlush = true;
  ++totalDataEvents;
  await sortProvider.addEvent(event);
  return checkFlushInterval(); // check if we can return a preview
};

/**
 * Helper to check if we should flush a sort preview.
 * We check if a preview needs to be flushed all SORT_FLUSH_INTERVAL milliseconds.
 * @returns The preview if it was time to flush or null otherwise
 */
function checkFlushInterval() {
  let ret = null;

  if (checkFlush && !hadFinal && !suppressPreviews) {
    const now = Date.now();
    checkFlush = false;

    let delayNextCheckMs = SORT_FLUSH_INTERVAL;
    if (lastFlush) {
      const since = now - lastFlush;
      logger.debug('checking for implicit sort preview flush', { delta: since });
      if (!externalFlushTrigger) {
        if (since >= SORT_FLUSH_INTERVAL) {
          ret = flushPreview();
          delayNextCheckMs = SORT_FLUSH_INTERVAL - (Date.now() - now); // flush might have taken some time...
        } else {
          // too early (i.e. timer signal event)
          delayNextCheckMs = SORT_FLUSH_INTERVAL - since;
        }
      }
    } else {
      // initializing lastFlush with first event
      lastFlush = now;
      timeToFinal = now;
    }

    // lets check in delayNextCheckMs milliseconds again
    if (!externalFlushTrigger) {
      logger.debug('resetting timer for next preview flush', { delay: delayNextCheckMs });
      setTimeout(() => (checkFlush = true), delayNextCheckMs);
    }
  }

  return ret?.length ? ret : null;
}

/**
 * Produces an array of sorted preview events.
 * @param Optional complete_gen event that causes this flush
 * @returns The preview events or empty array if there is nothing to sort
 */
function flushPreview(completeSource) {
  if(suppressPreviews) return [];
  // time to flush, return [reset, <...sorted preview...>, complete_gen]
  hadDataSinceFlush = false;
  // IMPORTANT!  The events returned by preview() are the actual live events, not clones.  Since downstream
  // functions in the pipeline can mutate events, we must ensure we're sending clones downstream, not the
  // originals.  Otherwise, every time we emit previews, the mutations would "accumulate" unintentionally.
  // By sending clones, downstream mutations have zero effect on the final results.
  const ret = sortProvider.preview().map((e) => e.__clone());
  if (lastPreviewEvents = ret.length) {   
    const resetEvent = ret[0].__clone(false, []);
    resetEvent.__signalEvent__ = 'reset';
    resetEvent.__setCtrlField('sort', 'preview'); // marking for debugging
    ret.unshift(resetEvent);

    // finish the batch off with a complete_gen event to indicate that we're done with this preview
    if (completeSource) {
      ret.push(completeSource);
    } else {
      const completeGenEvent = resetEvent.__clone(false, []);
      completeGenEvent.__signalEvent__ = 'complete_gen';
      ret.push(completeGenEvent);
    }

    ++previewCount;
    logger.debug('created sorted preview', {
      id: sortProvider.conf.id,
      events: ret.length - 2, // minus reset and complete_gen signals
      previews: previewCount,
    });
  }

  lastFlush = Date.now();
  return ret;
}

/**
 * Called to switch from internal (time-based) trigger to flush sort previews
 * to an external trigger. This is for example the case if we found reset/complete
 * events from some other function that pushes previews in batches.
 * @param reasonEvent The signal event, causing the switch
 */
function useExternalFlushTrigger(reasonEvent) {
  if (!externalFlushTrigger) {
    externalFlushTrigger = true;
    logger.debug('switching to external flush trigger', {event:reasonEvent});
  }
}

/**
 * Clean up on UnLoad.
 */
exports.unload = () => {
  logger.debug('unloading sort function', { id: sortProvider.conf.id });
  sortProvider.reset();
};
