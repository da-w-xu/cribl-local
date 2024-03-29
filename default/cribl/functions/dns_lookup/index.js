const cLogger = C.util.getLogger('func:dns_lookup');
const { NestedPropertyAccessor } = C.expr;
const NodeCache = C.internal.NodeCache;
const ResolveFileReader = C.internal.ResolveFileReader;

const NOOP = () => {};

const dns = require('dns').promises;
const net = require('net');

exports.name = 'DNS Lookup';
exports.version = '0.2';
exports.group = 'Standard';

// Cache
let _cache;
const CACHE_MAX_SIZE = 100000;

// Input to Output field mappings:
let _reverseLookupFields = [];
let _dnsLookupFields = [];

// Overrides to global DNS
let _dnsServers;
let _dnsResolver;

//override domain
let _domainOverrides = [];
let _resolveFileDomains = null;
let useResolveFile = false;
let useLookup = false;

// Used to throttle error logging.
let numErrors = 0;
const ERROR_THRESHOLD = 1000;

exports.init = (opts) => {
  const conf = opts.conf || {};
  conf.dnsLookupFields = conf.dnsLookupFields || [];
  conf.reverseLookupFields = conf.reverseLookupFields || [];
  if (!conf.dnsLookupFields.length && !conf.reverseLookupFields.length) {
    throw new Error('Invalid arguments - Must specify at least 1 field to lookup!', conf);
  }

  useLookup = conf.lookupFallback || false;
  useResolveFile = conf.useResolvConf || false;
  _dnsResolver = dns;
  _dnsServers = conf.dnsServers || [];
  if (_dnsServers.length) {
    try {
      _dnsResolver = new dns.Resolver();
      _dnsResolver.setServers(_dnsServers);
      cLogger.info('Using DNS overrides.', { dnsServers: _dnsServers });
    } catch (err) {
      cLogger.error('Invalid DNS override(s)', { dnsServers: conf.dnsServers, err });
      throw new Error(
        'Invalid arguments - DNS overrides contain one or more invalid IP addresses! Check the logs for more details.',
        conf
      );
    }
  }

  setUpFields(conf.reverseLookupFields, _reverseLookupFields);
  setUpFields(conf.dnsLookupFields, _dnsLookupFields, true);

  conf.maxCacheSize = Number(conf.maxCacheSize);
  conf.maxCacheSize = Number.isNaN(conf.maxCacheSize) ? 5000 : conf.maxCacheSize;
  if (conf.maxCacheSize > CACHE_MAX_SIZE)
    throw new Error(`Invalid argument - Max cache size allowed is ${CACHE_MAX_SIZE}`, conf);
  conf.cacheTTL = Number(conf.cacheTTL);
  conf.cacheTTL = Number.isNaN(conf.cacheTTL) ? 30 : conf.cacheTTL;

  _domainOverrides = conf.domainOverrides || [];

  if (conf.cacheTTL > 0) {
    _cache = new NodeCache({
      stdTTL: conf.cacheTTL * 60,
      checkperiod: 600,
      useClones: false,
      maxKeys: conf.maxCacheSize,
    });
  }
};

exports.process = (event) => {
  const promises = [];
  promises.push(...reverseDnsLookup(event));
  promises.push(...dnsLookup(event));
  return Promise.all(promises).then(() => event);
};

exports.unload = () => {
  _cache = undefined;
  _dnsLookupFields = [];
  _reverseLookupFields = [];
  _dnsServers = undefined;
  _dnsResolver = undefined;
  _domainOverrides = [];
  _resolveFileDomains = null;
  useResolveFile = false;
  useLookup = false;

};

function getDnsResolver() {
  return _dnsResolver;
}

async function readResolveFileAndStoreInCache() {
  return ResolveFileReader.readResolveFile(cLogger).then((domains) => {
    cLogger.info("resolv.conf domains added ", {'domains': domains})
    _resolveFileDomains = domains;
  }).catch(e => {
    cLogger.warn(`failed to read '${ResolveFileReader.resolvFile}')`, { reason: e });
    return Promise.reject(e);
  });
}

const reverseFn = (_ip) => getDnsResolver().reverse(_ip);

function reverseDnsLookup(event) {
  const results = [];
  for (let i = 0; i < _reverseLookupFields.length; i++) {
    const [inputField, outputField] = _reverseLookupFields[i];
    const ip = (inputField.get(event) || '').trim();
    if (net.isIP(ip)) {
      const key = `ip_${ip}`;
      const p = resolve(key, reverseFn, [ip])
        .then((val) => {
          if (val !== null) outputField.set(event, val);
        })
        .catch(NOOP);
      results.push(p);
    }
  }
  return results;
}

function lookupWithResolveFile(_dns, _rrt) {
  // if this is the first time looking it up from the resolv.conf file, pull the domains from the file, and store them
  // in _resolveFileDomains
  // otherwise, try to resolve the shortnames with the domains listed in _resolveFileDomains
  if (_resolveFileDomains === null) {
    return readResolveFileAndStoreInCache().
      then(() => shortnameWithDomainOverrideLookup(_dns, _rrt, _resolveFileDomains));
  } else {
    return shortnameWithDomainOverrideLookup(_dns, _rrt, _resolveFileDomains);
  }
}

const lookupFn = async (_dns, _rrt) => {
  // 1. attempt to resolve using dns.resolve
  try {
    const records = await getDnsResolver().resolve(_dns, _rrt);
    return extractRecords(records, _rrt);
  } catch (err) {
    if (!useResolveFile && !_domainOverrides.length && !useLookup) throw err;
  }

  // 2. fallback on resolv.conf
  if (useResolveFile) {
    try {
      return lookupWithResolveFile(_dns, _rrt);
    } catch (err) {
      if (!_domainOverrides.length && !useLookup) throw err;
    }
  }

  // 3. fallback on domain overrides
  if (_domainOverrides.length) {
    try {
      return shortnameWithDomainOverrideLookup(_dns, _rrt, _domainOverrides);
    } catch (err) {
      if (!useLookup) throw err;
    }
  }

  // 4. fallback on dns.lookup
  return dns.lookup(_dns, _rrt).then(results => extractFromLookupCall(results, _rrt));
};


function extractRecords(records, _rrt) {
  if (_rrt !== 'ANY') return records;
  if (!Array.isArray(records)) return records;
  if (records.length === 1) return records;
  const _rec = {};
  for (let x = 0; x < records.length; x++) {
    const {type, ...rest} = records[x];
    if (!_rec[type]) _rec[type] = rest;
    else if (Array.isArray(_rec[type])) _rec[type].push(rest);
    else _rec[type] = [_rec[type], rest];
  }
  return _rec;
}

function extractFromLookupCall(records, _rrt) {
  const recmap = [];
  if (!Array.isArray(records)) {
    records = [records];
  }
  for (let i = 0; i < records.length; i++) {
    recmap.push(records[i].address);
  }
  return extractRecords(recmap, _rrt);
}

async function shortnameWithDomainOverrideLookup(_dns, _rrt, domains) {
  for (const domainOverride of domains) {
    //ignore '.' for domain override as we've already done a lookup of _dns
    if (domainOverride === '.') {
      continue;
    }
    const fulldns = `${_dns}.${domainOverride}`;
    cLogger.debug(`fallback looking up ${fulldns}`);
    try {
      const results = await dns.resolve(fulldns, _rrt)
      if (results.length > 0) {
        return extractRecords(results, _rrt);
      }
    } catch (e) {
      //ignore err, move on to the next domain
    }

  }
  throw new Error('fallback dns resolution found no results');
}

function dnsLookup(event) {
  const results = [];
  for (let i = 0; i < _dnsLookupFields.length; i++) {
    const [inputField, outputField, resourceRecordType] = _dnsLookupFields[i];
    const domain = (inputField.get(event) || '').trim();
    if (!net.isIP(domain)) {
      const key = `dns_${domain}_${resourceRecordType}`;
      const p = resolve(key, lookupFn, [domain, resourceRecordType])
        .then((val) => {
          if (val !== null) outputField.set(event, val);
        })
        .catch(NOOP);
      results.push(p);
    }
  }
  return results;
}

const CACHE_THRESHOLD = 5000;
async function resolve(cacheKey, pullFn, args) {
  if (_cache) {
    const cached = _cache.get(cacheKey);
    if ((_cache.stats.hits + _cache.stats.misses) % CACHE_THRESHOLD === 0) {
      cLogger.debug('Cache stats', _cache.getStats());
    }
    if (cached) return cached;
  }
  const p = new Promise((res) => {
    const start = Date.now();
    pullFn(...args)
      .then((result) => {
        if (Array.isArray(result) && result.length === 1) result = result[0];
        res(result);
      })
      .catch((err) => {
        handleError(err, { args });
        res(null);
      })
      .finally(() => cLogger.debug(`dns took ${Date.now() - start}`, { args }));
  });
  if (_cache) _cache.set(cacheKey, p);
  return p;
}

const handleError = (error, info) => {
  if (numErrors++ % ERROR_THRESHOLD) {
    cLogger.error('DNS Lookup error', { error, ...info });
  }
};

function setUpFields(source, dest, withDnsRecords) {
  for (let i = 0; i < source.length; i++) {
    const field = source[i];
    const { inFieldName, outFieldName, resourceRecordType } = field;
    if (inFieldName && inFieldName.length) {
      const outputField = outFieldName && outFieldName.length ? outFieldName : inFieldName;
      const fields = [new NestedPropertyAccessor(inFieldName), new NestedPropertyAccessor(outputField)];
      if (withDnsRecords) fields.push(resourceRecordType || 'A');
      dest.push(fields);
    }
  }
}

if (process.env.NODE_ENV === 'test') {
  // For unit test
  exports.getDnsCache = () => _cache;
  exports.getDnsResolver = getDnsResolver;
}
