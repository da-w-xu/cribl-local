exports.name = 'Chain';
exports.version = '1.3';
exports.cribl_version = '3.2.0';
exports.disabled = false;
exports.handleSignals = true;
exports.group = 'Advanced'
exports.asyncTimeout = -1

let logger;

let processorId;
let signature;

let processor;

exports.init = async opts => {
  processorId = opts.conf?.processor;
  signature = `${opts.cid}:${opts.pid}${opts.pipeIdx != null ? ':' + opts.pipeIdx : ''}`;
  logger = C.util.getLogger('func:chain', {signature, processorId});
  logger?.info('creating new processor');
  try {
    processor = await C.internal.getEventProcessor(processorId);
  } catch (reason) {
    logger.warn('failed to create event processor, function will act as a pass thru', {reason});
    exports.sync = true;
  }
  // we verify that the processor implements isSync before calling it
  if (processor?.isSync) {
    exports.sync = processor?.isSync();
  }
}

exports.process = event => {
  if (processor == null || processor.isClosed()) {
    // if we don't have a processor, it could be because:
    //  - the processor has been removed but the configuration of this chain function was not updated
    return event;
  }
  const prom = processor.process(event);
  if (exports.sync) {
    // if the processor is synchronous, we just unwrap the FastPromise synchronously
    let evt;
    prom.then(e => evt = e);
    return evt;
  }
  return prom;
}

exports.unload = () => {
  logger?.info('closing processor');
  processor?.close();
  processor = null;
}
