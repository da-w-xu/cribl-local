exports.jobType = 'task-per-node';
exports.name = 'worker_upgrade';

const os = require('os');

let upgradeClient;
let packages;
let authToken;

const {
  internal: { performPackageDownload },
} = C;

exports.initJob = async (opts) => {
  const { conf } = opts.conf.executor;
  
  packages = [];
  await Promise.all(conf.packages.map( async (each) => {
    const { packageFile, packageUrl, hashUrl, hashFile, hashType } = each;
    try {
      await performPackageDownload(packageUrl, packageFile, hashUrl, hashFile, hashType);
      packages.push(each);
    } catch { /* ignored */ }
  }))
  if (!packages.length) throw new Error('Failed to download packages.');

  authToken = conf.authToken;
};
exports.jobSeedTask = async () => {
  return {
    task: {
      packages,
      authToken,
    },
  };
};
exports.initTask = async (opts) => {
  // don't break backwards compatibility. Remember: this task may be executed on pre-4.3.x nodes
  if(C.internal.newUpgradeClient) {
    upgradeClient = C.internal.newUpgradeClient();
  } else {
    upgradeClient = new C.internal.UpgradeClient();
  }
};

exports.jobOnError = async (job, taskId, error) => {}; 

exports.taskExecute = async (job, opts) => {
  const logger = job.logger();
  const variant = [os.platform(), os.arch()];
  const package = opts.packages.find((p) => p.variant[0] === variant[0] && p.variant[1] === variant[1]);

  if(!package) {
    job.reportError(new Error(`Could not find a suitable package for ${variant.join(', ')}`), 'TASK_FATAL');
    return;
  }

  logger.info('task opts', { opts });

  let upgradeResult;
  // Leader will deploy this job starting with 4.7.0, so even older nodes will try this `if`
  // but it will only be true on 4.7.0+ remote nodes
  if (typeof upgradeClient.performUpgrade === 'function') {
    // 4.7.0 remote nodes and newer handle upgrade as defined in their binary
    // this allows us vary upgrade behavior per version, without dealing with versioning this conf file
    upgradeResult = await upgradeClient.performUpgrade(package, opts, logger);
    if (!upgradeResult.canUpgrade) {
      logger.info(upgradeResult.message);
      job.addResult(upgradeResult);
      return;
    }
  } else {
    // remote nodes older than 4.7.0 run this path
    const descriptor = {
      packageUrl: package.localPackageUrl,
      hashUrl: package.localHashUrl,
      version: package.version,
    };
    logger.info('Checking upgradeability', descriptor);
    upgradeResult = await upgradeClient.checkUpgradePath(descriptor, logger);
    if (!upgradeResult.canUpgrade) {
      logger.info(upgradeResult.message);
      job.addResult(upgradeResult);
      return;
    }
    logger.info('Fetching assets');
    const downloadResult = await upgradeClient.downloadAssets(descriptor, opts.authToken);
    logger.info('Fetched assets', downloadResult);
    if (descriptor.hashUrl) {
      logger.info('Verifying assets');
      await upgradeClient.verifyAssets(downloadResult);
      logger.info('Assets verified');
    }
    logger.info('Proceeding to installation');
    upgradeResult = await upgradeClient.installPackage(downloadResult, upgradeResult);
  }

  logger.info(upgradeResult.message);
  if (!upgradeResult.isSuccess) {
    job.reportError(new Error(upgradeResult.message), 'TASK_FATAL');
    return;
  }
  await job.addResult(upgradeResult);

  setImmediate(() => upgradeClient.restartServer().catch(() => {}));
};
