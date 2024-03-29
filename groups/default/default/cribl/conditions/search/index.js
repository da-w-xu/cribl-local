exports.name = 'Search Condition';
exports.type = 'message';
exports.category = 'search';
let notificationId;
exports.init = (opts) => {
  notificationId = opts.conf.__notificationId;
};
exports.build = () => ({
  filter: `id.startsWith('SEARCH_NOTIFICATION_${notificationId}')`,
  pipeline: {
    conf: {
      functions: [
        {
          // expand metadata into the event itself to facilitate notification meta field extraction
          filter: 'true',
          conf: {
            maxNumOfIterations: 5000,
            code: 'const meta = __e.metadata;\nfor(const {key, value} of meta) {\n    if(key && value != null) __e[key] = value;\n}\n__e.metadata = undefined',
          },
          id: 'code',
        },
      ],
    },
  },
});
