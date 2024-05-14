exports.name = 'Persistent Queue Usage';
exports.type = 'metric';
exports.category = 'destinations';

let id;
let name;
let __workerGroup;
let timeWindow;
let usageThreshold;
let usageThresholdDecimal;
let notifyOnResolution;

exports.init = (opts) => {
  id = opts.pid;
  const conf = opts.conf || {};
  ({
    name,
    __workerGroup,
    timeWindow,
    usageThreshold,
    notifyOnResolution,
  } = conf);
  timeWindow = timeWindow || '60s';
  usageThreshold = usageThreshold != null ? usageThreshold : 90; // percentage value (0-99)
  usageThresholdDecimal = usageThreshold / 100;

  // keep default false for existing customers with this notification.
  // customers keep old behavior.
  notifyOnResolution = notifyOnResolution || false;
};

exports.build = () => {
  let workerGroupMessage = "";
  let filter = `_metric === 'system.pq_used' && output === '${name}'`;
  let msg_alarm = `'Destination ${name} persistent queue usage greater than ${usageThreshold}% threshold in ${timeWindow}'`;
  let msg_ok = `'Destination ${name} persistent queue usage is within ${usageThreshold}% threshold in ${timeWindow}'`;

  if (__workerGroup) {
    filter = `${filter} && __worker_group === '${__workerGroup}'`;
    workerGroupMessage = ` in group ${__workerGroup}`;
    msg_alarm = `'Destination ${name}${workerGroupMessage} persistent queue usage greater than ${usageThreshold}% threshold in ${timeWindow}'`;
    msg_ok = `'Destination ${name}${workerGroupMessage} persistent queue usage is within ${usageThreshold}% threshold in ${timeWindow}'`;
  }

  return {
    filter,
    pipeline: {
      conf: {
        functions: [
          {
            id: 'aggregation',
            conf: {
              timeWindow,
              aggregations: [
                'last(_value).as(queue_usage)'
              ],
              lagTolerance: '20s',
              idleTimeLimit: '20s',
            }
          },
          {
            id: 'eval',
            conf: {
              add : [
                { name: 'output', value: `'${name}'` },
                { name: 'timewindow', value: `'${timeWindow}'` },
                { name: 'usageThreshold', value: `'${usageThreshold}'` },
                { name: '_metric', value: "'system.pq_used'" },
                { name: 'usage', value: '`${queue_usage * 100}`' },
              ]
            }
          },
          {
            id: 'eval',
            filter: `queue_usage > ${usageThresholdDecimal}`,
            conf: {
              add : [
                { name: '_raw', value: `${msg_alarm}`},
                { name: 'status', value: `'ALARM'`},
              ]
            }
          },
          {
            id: 'eval',
            filter: `typeof queue_usage === 'undefined' || queue_usage <= ${usageThresholdDecimal}`,
            conf: {
              add: [
                { name: '_raw', value: `${msg_ok}`},
                { name: 'status', value: `'OK'`}
              ]
            }
          },
          {
            id: 'notifications',
            conf: {
              id: id,
              field: 'status',
              deduplicate: notifyOnResolution
            }
          },
          {
            id: 'drop',
            filter: `!(${notifyOnResolution}) && status === 'OK'`,
          },
        ]
      }
    }
  };
};
