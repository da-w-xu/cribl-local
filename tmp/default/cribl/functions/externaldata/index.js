exports.name = 'Externaldata';
exports.version = '0.1';
exports.handleSignals = false;
exports.group = C.INTERNAL_FUNCTION_GROUP;
exports.sync = true;

exports.init = () => {};

// NOOP, just passes objects through to the next thing.
exports.process = (event) => {
  return event;
};
