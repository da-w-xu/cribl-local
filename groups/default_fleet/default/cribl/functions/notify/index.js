exports.name = 'notify';
exports.version = '0.0.1';
exports.disabled = false;
exports.handleSignals = true;
exports.group = C.INTERNAL_FUNCTION_GROUP;

let group,
  searchId,
  savedQueryId,
  notificationId,
  message,
  messageTemplate,
  authToken,
  messagesEndpoint,
  searchUrl,
  logger,
  comparatorExpression,
  trigger = 'true',
  triggerCount = 0,
  triggerExpression,
  resultsLimit = 10,
  triggerCounter=0,
  triggerType= 'resultsCount', 
  triggerComparator = '>', 
  notificationResults = [], 
  notificationSent= false,
  signalCounter=0,
  utLogger=undefined,
  tenantId=undefined;

const { RestVerb } = C.internal.HttpUtils;
const { createRequest } = C.internal.kusto;

const BulletinMessage = (now, notificationId, message, searchId, savedQueryId, searchUrl, tenantId) => {
  return {
    id: `SEARCH_NOTIFICATION_${notificationId}_${now}`,
    severity: 'info',
    text: message,
    title: `Scheduled search notification`,
    time: now,
    group,
    // search notification condition expects metadata to be populated
    metadata: [
      {
        itemType: 'link',
        id: searchId,
        type: 'search',
        product: 'search',
      },
      {
        key: 'searchId',
        value: searchId,
      },
      {
        key: 'savedQueryId',
        value: savedQueryId,
      },
      {
        key: 'searchResultsUrl',
        value: searchUrl,
      },
      {
        key: '__bulletinIgnore',
        value: true,
      },
      {
        key: 'tenantId',
        value: tenantId,
      },
    ]
  };
};
const comparators =  [">", "<", "===", "!==", ">=", "<="];
exports.init = async (opt) => {
  // reset defaults for testing
  trigger = 'true';
  triggerCount = 0;
  triggerExpression;
  resultsLimit = 10;
  triggerCounter=0;
  triggerType= 'resultsCount';
  triggerComparator = '>';
  notificationResults = [];
  const conf = opt.conf;
  ({ searchId, message, savedQueryId, authToken, messagesEndpoint, searchUrl, utLogger, notificationId, tenantId} =
    conf);
  logger = utLogger ?? C.util.getLogger(`func:notify:${searchId}`);
  messageTemplate = new C.internal.kusto.Template(message, false, logger);
  group = conf.group ?? group;
  trigger = conf.trigger ?? trigger;
  triggerExpression = new C.expr.Expression(trigger);
  resultsLimit = conf.resultsLimit ?? resultsLimit;
  triggerCount = conf.triggerCount ?? triggerCount;
  triggerType = conf.triggerType ?? triggerType;
  triggerComparator = conf.triggerComparator ?? triggerComparator;

  if(!comparators.includes(triggerComparator)){
    throw new Error(`Unknown comparator ${triggerComparator}`)
  }
  // parse the comparator into comparison fn.
  comparatorExpression = new C.expr.Expression(`triggerCounter ${triggerComparator} triggerCount`);
  logger.info('Initialized notify', {
    ...conf,
  });
};

exports.process = async (event) => {
  if(event.__signalEvent__ === 'final' && !notificationSent) {
    // evaluate comparator on final, since since the comparison op might be <= 
    if (comparatorExpression.evalOn({triggerCounter, triggerCount})) {
      await sendNotification(notificationResults);
    }
  }
  if(event.__signalEvent__ === 'reset'){
    signalCounter++;
    // we always emit at least 1 reset/complete signal in aggregations
    if(signalCounter > 1) logger.error('Signal event received in notify pipeline function, which cannot handle previews', {signalCounter});
  }
  if (event.__signalEvent__ != null || (triggerType === 'custom' && !triggerExpression.evalOn(event))) return event;
  triggerCounter++;
  if (notificationResults.length < resultsLimit) notificationResults.push(event);
  return event;
};
/**
 * Send the notification to the bulletin message endpoint.
 * @param {CriblEvent[]} notificationResults 
 * @returns Promise<void>
 */
async function sendNotification(notificationResults) {
  // fail safe in case of receiving final twice
  notificationSent = true;
  const now = new Date();
  const message = messageTemplate.render({
    resultSet: notificationResults,
    savedQueryId,
    searchId,
    searchResultsUrl: searchUrl,
    notificationId,
    timestamp: now.toISOString(),
    tenantId,
  });
  const notificationBulletinEvent = BulletinMessage(
      now.getTime(),
      notificationId,
      message,
      searchId,
      savedQueryId,
      searchUrl,
      tenantId
    );
  await sendBulletinMessage(
    notificationBulletinEvent
  );
}

function sanitizeMsg(bulletinMessage) {
  const copy = Object.assign({}, bulletinMessage);
  // remove results from log message
  const [_, ...rest] = copy.metadata.reverse();
  copy.metadata =rest;
  return copy;
}
async function sendBulletinMessage(bulletinMessage) {
  try {
    logger.debug('Sending message', { bulletinMessage: sanitizeMsg(bulletinMessage) });
    const opts = {
      url: messagesEndpoint,
      method: RestVerb.POST,
      payload: bulletinMessage,
    };
    const rv = await createRequest(opts).addAuthToken(authToken).run();
    await rv.readAsJSON();
  } catch (error) {
    logger.error('Error posting notification message', { error });
  }
}

exports.unload = () => {
    messageTemplate?.dispose();
    triggerExpression = undefined;
    notificationResults= undefined;
};
