exports.name = 'Unhealthy Destination';
exports.type = 'metric';
exports.category = 'destinations';

let id;
let name;
let __workerGroup;
let timeWindow;
let notifyOnResolution;
exports.init = (opts) => {
  const conf = opts.conf || {};
  ({
    id = opts.pid,
    name,
    __workerGroup,
    timeWindow,
    notifyOnResolution
  } = conf);
  timeWindow = timeWindow || '60s';
  // keep default false for existing customers with this notification.
  // customers keep old behavior.
  notifyOnResolution = notifyOnResolution || false;
};

exports.build = () => {
  let filter = `(_metric === 'health.outputs' && output === '${name}')`;
  let msg_alarm = `'Destination ${name} is unhealthy in the last ${timeWindow}'`;
  let msg_ok = `'Destination ${name} is healthy in the last ${timeWindow}'`;

  if (__workerGroup) {
    filter = `${filter} && __worker_group === '${__workerGroup}'`;
    msg_alarm = `'Destination ${name} in group ${__workerGroup} is unhealthy in the last ${timeWindow}'`;
    msg_ok = `'Destination ${name} in group ${__workerGroup} is healthy in the last ${timeWindow}'`;
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
                'perc(95, _value).as(health)'
              ],
              lagTolerance: '20s',
              idleTimeLimit: '20s',
            }
          },
          {
            id: 'eval',
            conf: {
              add: [
                { name: 'output', value: `'${name}'` },
                { name: 'timewindow', value: `'${timeWindow}'` },
                { name: '_metric', value: "'health.outputs'" }
              ]
            }
          },
          {
            id: 'eval',
            filter: 'Math.round(health) < 2',
            conf: {
              add : [
                { name: '_raw', value: `${msg_ok}`},
                { name: 'status', value: `'OK'`},
              ]
            }
          },
          {
            id: 'eval',
            filter: 'Math.round(health) >= 2',
            conf: {
              add : [
                { name: '_raw', value: `${msg_alarm}`},
                { name: 'status', value: `'ALARM'`},
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