/* eslint-disable no-await-in-loop */

exports.name = 'Health Check';
exports.version = '0.1';
exports.disabled = false;
exports.destroyable = false;

const { HealthCheckCollector } = C.internal.Collectors;
const healthCheck = new HealthCheckCollector();

function getCollectorConfig(opts) {
  const conf = opts.conf;
  this.authenticateCollect = conf.authenticateCollect ?? false;

  const collectorConf = {
    ...conf,
    "pagination": { "type": "none" },
    "authenticateCollect": this.authenticateCollect,
    "disableTimeFilter": conf.disableTimeFilter ?? true,
    "useRoundRobinDns": conf.useRoundRobinDns ?? false,
    retryRules: { type: 'backoff', codes: [ 429, 503 ], ...conf.retryRules }
  }

  opts.conf = collectorConf ;
}

exports.init = async (opts) => {
  getCollectorConfig(opts);
  return healthCheck.initEx(opts);
};

exports.discover = async (job) => {
  return healthCheck.discover(job);
};

exports.collect = async (collectible, job) => {
  return healthCheck.collect(collectible, job);
};

exports.getParser = (job) => {
  return healthCheck.getParserEx(job);
};
