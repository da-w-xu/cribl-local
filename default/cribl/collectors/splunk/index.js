exports.name = 'Splunk Search';
exports.version = '0.1';
exports.disabled = false;
exports.destroyable = false;

const { DEFAULT_TIMEOUT_SECS } = C.internal.HttpUtils;
const { RestCollector } = C.internal.Collectors;
const restC = new RestCollector();

function normalizeSearchQuery(searchQuery) {
  if (!searchQuery) throw new Error('Missing required parameter search');
  searchQuery = searchQuery.trim();
  // For Splunk's search jobs endpoint(s), queries must begin with either "search" or "|" tokens.
  if (searchQuery.startsWith('search ') || searchQuery.startsWith('|')) {
    return searchQuery;
  } else {
    return `search ${searchQuery}`;
  }
}

function getCollectMethod(endpoint) {
  // We need to preserve the original behavior which only used 'get' requests, but also support the v2 endpoint
  // which only accepts 'post' requests.
  // https://docs.splunk.com/Documentation/Splunk/9.0.3/RESTREF/RESTsearch#search.2Fv2.2Fjobs.2Fexport
  if (endpoint?.includes('v2') && endpoint?.includes('export')) {
    return 'post';
  } else {
    return 'get';
  }
}

function getCollectorConfig(opts) {
  const conf = opts.conf;
  let searchHeadAddr = C.expr.runExprSafe(conf.searchHead)
  if (searchHeadAddr.endsWith('/')) searchHeadAddr.substr(0,searchHeadAddr.length-1);
  let searchHeadEndpoint = C.expr.runExprSafe(conf.endpoint)
  if (!searchHeadEndpoint.startsWith('/')) searchHeadEndpoint = `/${searchHeadEndpoint}`;
  const searchHead = `${searchHeadAddr}${searchHeadEndpoint}`;
  const collectorConf = {
    "discovery": { "discoverType": "none" },
    "collectMethod": getCollectMethod(searchHeadEndpoint),
    "authentication": conf.authentication,
    "collectUrl": searchHead,
    "collectRequestParams": [{
      "name": "search",
      "value": normalizeSearchQuery(C.expr.runExprSafe(conf.search))
    }],
    "collectRequestHeaders": [
      { "name": "accept", "value": "'application/json'" }
    ],
    "username": conf.username,
    "password": conf.password,
    "filter": "(true)",
    "discoverToRoutes": false,
    "timeout": !isNaN(conf.timeout) ? conf.timeout : DEFAULT_TIMEOUT_SECS,
    "disableTimeFilter": conf.disableTimeFilter ?? true,
    "useRoundRobinDns": conf.useRoundRobinDns ?? false,
    "rejectUnauthorized": conf.rejectUnauthorized ?? false,
    "retryRules": conf?.retryRules || { type: 'backoff', codes: [429, 503] }
  }

  if (conf.earliest) {
    collectorConf.collectRequestParams.push({
      "name": "earliest_time",
      "value": conf.earliest
    });
  }
  if (conf.latest) {
    collectorConf.collectRequestParams.push({
      "name": "latest_time",
      "value": conf.latest
    });
  }
  let outputMode = conf.outputMode || 'json';
  collectorConf.collectRequestParams.push({
    "name": "output_mode",
    "value": outputMode
  });

  collectorConf.collectRequestParams.push(...(conf.collectRequestParams ?? []));
  collectorConf.collectRequestHeaders.push(...(conf.collectRequestHeaders ?? []));

  if ((conf.authentication === 'token') || (conf.authentication === 'tokenSecret')) {
    collectorConf.token = conf?.token;
    if ((! collectorConf.token.startsWith('Bearer ')) && (! collectorConf.token.startsWith('Splunk '))) {
      collectorConf.token = 'Bearer '.concat(collectorConf.token);
    }
    delete collectorConf['password'];
    delete collectorConf['username'];
  }
  return { conf: collectorConf };
}

exports.init = async (opts) => {
  const config = getCollectorConfig(opts);
  return restC.init(config);
}

exports.discover = async (job) => {
  return restC.discover(job);
}

exports.collect = async (collectible, job) => {
  return restC.collect(collectible,job);
}

exports.getParser = (job) => {
  return restC.getParser(job);
};
