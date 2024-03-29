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
  logger.info('Checking upgradeability', package);
  let upgradeResult;
  upgradeResult = await upgradeClient.checkUpgradePath(package, job.logger());
  if (!upgradeResult.canUpgrade) {
    logger.info(upgradeResult.message);
    job.addResult(upgradeResult);
    return;
  }
  logger.info('Fetching assets');
  const downloadResult = await upgradeClient.downloadAssets(package, opts.authToken);
  logger.info('Fetched assets', downloadResult);
  if (package.hashUrl) {
    logger.info('Verifying assets');
    await upgradeClient.verifyAssets(downloadResult);
    logger.info('Assets verified');
  }
  logger.info('Proceeding to installation');
  upgradeResult = await upgradeClient.installPackage(downloadResult, upgradeResult);
  logger.info(upgradeResult.message);
  if (!upgradeResult.isSuccess) {
    job.reportError(new Error(upgradeResult.message), 'TASK_FATAL');
    return;
  }
  await job.addResult(upgradeResult);

  setImmediate(() => upgradeClient.restartServer().catch(() => {}));
};