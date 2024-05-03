/* eslint-disable no-await-in-loop */
exports.name = 'S3';
exports.version = '0.2';
exports.disabled = false;
exports.destroyable = false;

const { S3Collector } = C.internal.Collectors;
const s3Collector = new S3Collector();

exports.init = (opts) => {
  const initProm = s3Collector.init(opts);
  exports.provider = s3Collector.provider;
  return initProm;
};

exports.discover = async (job) => {
  return s3Collector.discover(job);
};

exports.collect = async (collectible, job) => {
  return s3Collector.collect(collectible, job);
};

exports.close = async () => {
  await s3Collector.provider.close().catch((err)=>{/* NOOP */})
};