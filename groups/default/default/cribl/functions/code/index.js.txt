exports.name = 'Code';
exports.version = '0.2';
exports.disabled = false;
exports.group = 'Advanced';

const { Expression } = C.expr;
let cLogger; // init'ed as either default or unique below

let code;
let logger;
let numLogs = 0;
let activeLogSampleRate;

exports.init = (opts) => {
  const { conf, pid, pipeIdx } = opts;
  const { useUniqueLogChannel } = conf;
  if (useUniqueLogChannel) {
    const uniqueId = `func:code:${pid}:${pipeIdx}`;
    cLogger = C.util.getLogger(uniqueId);
  } else {
    cLogger = C.util.getLogger('func:code');
  }

  conf.code = conf.code ? conf.code.trim() : '';
  if (!conf.code) throw new Error(`Code can't be left empty!`);

  if (conf.maxNumOfIterations || conf.maxNumOfIterations === 0) {
    // validate only if it's set
    conf.maxNumOfIterations = Number(conf.maxNumOfIterations);
    conf.maxNumOfIterations = Number.isNaN(conf.maxNumOfIterations) ? -1 : conf.maxNumOfIterations;
    if (conf.maxNumOfIterations < 1 || conf.maxNumOfIterations > 10000) throw new Error(`The maximum number of iterations must be set between 1 and 10,000!`);
  }

  // set default value if activeLogSample rate is undefined
  numLogs = 0; // reset every init
  activeLogSampleRate = Number(conf.activeLogSampleRate ?? 1);
  if (Number.isNaN(activeLogSampleRate) || activeLogSampleRate < 1 || activeLogSampleRate > 5000) {
    throw new Error('The active log sample rate must be set between 1 and 5,000!');
  }

  // new Expression will throw in case an invalid code/expression has been passed around
  code = new Expression(`${conf.code}`, {unprotected: true, maxNumOfAllowedIterations: conf.maxNumOfIterations});

  logger = getLogger();

  return [{
    func: exports.name,
    severity: 'warn',
    message: `Has been enabled.`
  }];
};

exports.process = (event) => {
  if(!event || !code) return event;
  try {
    code.unsafeEvalOn(event, logger);
  } catch (err) {
    // Capture logs based on sample rate. By default it is set to 1:1
    if (numLogs++ % activeLogSampleRate === 0) {
      // report errors to either logs or pipeline preview
      cLogger.error("Error while executing Code function.", { error: err });
    }
  }
  return event;
};

function getLogger() {
  if (Boolean(process.env.CRIBL_PREVIEW))
    // pipeline preview is ran through the cli.
    // the preview log (shown in the UI) is built from what the cli process writes to stderr.
    // by default, loggers are initialized in info mode.
    // hence the need to log using this level (calling the debug method won't produce any ouptut).
    return (msg, obj={}) => cLogger.info(msg, obj);
  else
    // to avoid unintentionally spamming the logs, one have to set func's log level to debug
    // for seeing msgs in regular logs.
    return (msg, obj={}) => cLogger.debug(msg, obj);
}

if (process.env.NODE_ENV === 'test') {
  exports.getExpression = () => code;
}
