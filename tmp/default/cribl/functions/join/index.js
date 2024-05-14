exports.name = 'join';
exports.version = '0.0.1';
exports.disabled = false;
exports.group = C.INTERNAL_FUNCTION_GROUP;
exports.sync = true;

let joinProvider;

exports.init = (opt) => {
  const conf = opt.conf;

  joinProvider = C.internal.kusto.createJoin(conf);
};

exports.process = (event) => {
  return joinProvider.process(event);
};

exports.unload = () => {
  joinProvider = undefined;
}
