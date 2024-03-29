/* eslint-disable no-await-in-loop */

exports.name = 'Cribl Lake';
exports.version = '0.1';
exports.disabled = false;
exports.destroyable = false;

const { CriblLakeCollector } = C.internal.Collectors;
const collector = new CriblLakeCollector();

exports.init = (opts) => {
  // The only user configurable opt is dataset, AWS bucket and auth info are set to config at schedule time (see:
  // CollectionJob.ts). Remaining opts are hard-coded.
  const conf = {
    path: opts.conf.path,
    filter: opts.conf.filter,
    bucket: opts.conf.bucket,
    region: opts.conf.region,
    awsAuthenticationMethod: opts.conf.awsAuthenticationMethod,
    awsApiKey: opts.conf.awsApiKey,
    awsSecretKey: opts.conf.awsSecretKey,
    endpoint: opts.conf.endpoint,
    enableAssumeRole: opts.conf.enableAssumeRole ?? false,
    assumeRoleArn: opts.conf.assumeRoleArn,
    recurse: true,
    signatureVersion: 'v4',
    durationSeconds: 3600,
    reuseConnections: true,
    rejectUnauthorized: true,
    verifyPermissions: true,
    parquetChunkSizeMB: 5,
    parquetChunkDownloadTimeout: 600,
    maxBatchSize: 10,
  };
  opts.conf = conf;
  const initProm = collector.init(opts);
  exports.provider = collector.provider;
  return initProm;
};

exports.discover = async (job) => {
  return collector.discover(job);
};

exports.collect = async (collectible, job) => {
  return collector.collect(collectible, job);
};

exports.close = async () => {
  await collector.provider.close().catch((err)=>{/* NOOP */})
};
