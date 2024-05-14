exports.name = 'union';
exports.version = '0.0.1';
exports.disabled = false;
exports.handleSignals = true;
exports.group = C.INTERNAL_FUNCTION_GROUP;
exports.sync = true;

let unionProvider;

exports.init = (opt) => {
  const conf = opt.conf;

  unionProvider = C.internal.kusto.createUnion(conf);
};

exports.process = (event) => {
  return unionProvider.process(event);
};

exports.unload = () => {
  unionProvider = undefined;
};
