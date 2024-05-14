exports.name = 'Search Condition';
exports.type = 'search';
exports.category = 'search';
let notificationId;
exports.init = (opts) => {
  notificationId = opts.conf.__notificationId;
};
exports.build = () => ({
  filter: `id.startsWith('SEARCH_NOTIFICATION_${notificationId}')`,
  pipeline: {
    conf: {
      functions: [] // passthru
    }
  }
});
