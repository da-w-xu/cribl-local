exports.name = 'No Data Received';
exports.type = 'metric';
exports.category = 'sources';

let id;
let name;
let __workerGroup;
let timeWindow;
let notifyOnResolution;

exports.init = (opts) => {
  id = opts.pid;
  const conf = opts.conf || {};
  ({
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
  let filter = `(_metric === 'total.in_bytes' || _metric === 'health.inputs') && input === '${name}'`;
  let msgAlarm = `'Source ${name} had no traffic for ${timeWindow}'`;
  let msgOk = `'Source ${name} has started receiving traffic for ${timeWindow}'`;

  if(__workerGroup) {
    filter = `(${filter}) && __worker_group === '${__workerGroup}'`;
    msgAlarm = `'Source ${name} in group ${__workerGroup} had no traffic for ${timeWindow}'`;
    msgOk = `'Source ${name} in group ${__workerGroup} has started receiving traffic for ${timeWindow}'`;
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
                "sum(_value).where(_metric==='total.in_bytes').as(bytes)",
                "perc(95, _value).where(_metric==='health.inputs').as(health)"
              ],
              lagTolerance: '20s',
              idleTimeLimit: '20s',
            }
          },
          {
            id: 'eval',
            conf: {
              add : [
                { name: 'input', value: `'${name}'`},
                { name: '_metric', value: `'total.in_bytes'`},
                { name: 'timewindow', value: `'${timeWindow}'` }
              ]
            }
          },
          {
            id: 'eval',
            filter: "(typeof bytes === 'undefined' || bytes === 0)",
            conf: {
              add : [
                { name: '_raw', value: `${msgAlarm}`},
                { name: 'status', value: `'ALARM'`},
              ]
            }
          },
          {
            id: 'eval',
            filter: 'bytes > 0',
            conf: {
              add : [
                { name: '_raw', value: `${msgOk}`},
                { name: 'status', value: `'OK'`},
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
}