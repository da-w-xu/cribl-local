/**
 * Internal function to move secondary field into a nested pack.
 * All fields that are on the conf.unpackedFields remain at their current
 * position while everything else is moved into a sub-level object, who's
 * name is specified via conf.target.
 * (Used for Kusto pack(*) of find operator)
 */
exports.name = 'Pack';
exports.version = '0.1';
exports.handleSignals = false;
exports.group = C.INTERNAL_FUNCTION_GROUP;
exports.sync = true;

let unpackedFields;
let packTarget;

exports.init = (opts) => {
  const conf = opts.conf || {};

  unpackedFields = new Set(conf.unpackedFields ?? []);
  packTarget = conf.target ?? '_pack';
};

exports.process = (event) => {
  if (!event) return event;
  const ret = event.__clone(true, []); // clone w/o keys
  const packed = (ret[packTarget] = {});

  // split matching and non-matching properties into base and packed
  for (let key of Object.keys(event)) {
    if (key.startsWith('__') || unpackedFields.has(key)) {
      ret[key] = event[key];
    } else {
      packed[key] = event[key];
    }
  }

  return ret;
};
