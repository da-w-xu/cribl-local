/**
 * Cribl-internal function to record notification states & deduplicate based on state.
 */
exports.name = 'Notification state & deduplication';
exports.version = '0.1';
exports.handleSignals = false;
exports.group = C.INTERNAL_FUNCTION_GROUP;

let id;
let field;
let deduplicate;

exports.init = (opts) => {
  id = opts.conf.id;
  field = opts.conf.field;
  deduplicate = opts.conf.deduplicate;
  if (deduplicate === undefined) {
    deduplicate = true;
  }
};


exports.process = async (event) => {
  if (!event || !id || id === '' || !field || field === '') {
    return event;
  }
  const oldValue = await C.internal.NotificationsStateMgr.instance().get(id);
  const newValue = event[field] || null;
  await C.internal.NotificationsStateMgr.instance().set(id, newValue);
  if (deduplicate && oldValue === newValue) {
    return null;
  }
  return event;
};
