exports.name = 'Window';
exports.version = '0.1.0';
exports.disabled = false;
exports.handleSignals = true;
exports.group = C.INTERNAL_FUNCTION_GROUP;
exports.sync = true;

let logger;  // logger for this function
let eventWindow; // instance of the event window
let windowId; // stores the config's eventWindowId

/**
 * Initializes the window function
 * @param {*} opts Configuration for the window pipeline function
 */
exports.init = (opts) => {
  logger = C.util.getLogger(`func:window`, { eventWindowId: opts?.conf?.eventWindowId ?? 'n/a' });
  if (opts == null) {
    logger.error('window pipeline function w/o configuration');
    throw new Error('window pipeline function requires a configuration');
  }

  const { eventWindowId, registeredFunctions, tailEventCount, headEventCount } = opts?.conf ?? {};
  if (eventWindowId == null) {
    logger.error('required eventWindowId configuration missing', {opts});
    throw new Error('invalid configuration for window pipeline function');
  }

  if (!Array.isArray(registeredFunctions) || registeredFunctions.length < 1) {
    logger.error('required registeredFunctions configuration missing', {opts});
    throw new Error('invalid configuration for window pipeline function');
  }

  // get the event window from the window manager for the given ID
  windowId = eventWindowId; // keep it around for destroying on unload
  eventWindow =  C.kusto.windowManager(eventWindowId);
  if (eventWindow == null) {
    logger.error("window manager couldn't establish event window", {eventWindowId});
    throw new Error('event window manager not established');
  }

  // funcName.as(funcAlias) format extraction to register used window function instances to the window
  const nameRegex = /^(?<name>.+)\.as\((?<alias>.+)\)$/;
  registeredFunctions.forEach((funcEntry) => {
    logger.debug('registering function instance', { eventWindowId, funcEntry });
    const matches = funcEntry.match(nameRegex);
    if (matches) {
      eventWindow.registerWindowFunctionInst(matches[1], matches[2]);
    } else {
      eventWindow.registerWindowFunctionInst(funcEntry);
    }
  });

  // configure the event window
  if (tailEventCount) eventWindow.adjustTail(tailEventCount);
  if (headEventCount) eventWindow.adjustHead(headEventCount);

  logger.debug('initialization complete', { eventWindowId, tailEventCount: tailEventCount ?? 0, headEventCount: headEventCount ?? 0 });
}

/**
 * Processes a single pipeline event
 * @param {*} event single event
 * @returns the events to send downstream
 */
exports.process = (event) => {
  if (!event) return event;
  if (event.__signalEvent__) {
    logger.debug('received signal event', { signal: event.__signalEvent__ });
    switch(event.__signalEvent__) {
      case 'reset':
        eventWindow.reset();
        return event;

      case 'final':
      case 'complete_gen':
        const flushEvents = eventWindow.flushHead();
        logger.debug('flushing head events to output', { numEvents: flushEvents ? flushEvents.length : 'n/p' });
        return [...(flushEvents ?? []), event];
   
      default:
        return event;
    }
  }

  // not a signal -> pass it on to the event window
  return eventWindow.process(event);
}

/**
 * Unloads the function and destroys the event window
 */
exports.unload = () => {
  // windowId can only be undefined if we unload w/o init
  if (windowId != null) {
    C.kusto.windowManager().destroyWindow(windowId);
  }
}
