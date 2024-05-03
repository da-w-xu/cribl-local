exports.name = 'High Data Volume';
exports.type = 'metric';
exports.category = 'sources';

let id;
let name;
let __workerGroup;
let timeWindow;
let dataVolumeBytes;
let dataVolume;
let notifyOnResolution;

exports.init = (opts) => {
  id = opts.pid;
  const conf = opts.conf || {};
  ({
    name,
    __workerGroup,
    timeWindow,
    dataVolume,
    notifyOnResolution,
  } = conf);
  timeWindow = timeWindow || '60s';
  
  dataVolume = dataVolume || '1TB';
  dataVolumeBytes = C.util.parseMemoryStringToBytes(dataVolume, err => { throw err; });

  // keep default false for existing customers with this notification.
  // customers keep old behavior.
  notifyOnResolution = notifyOnResolution || false;
};

exports.build = () => {
  let filter = `(_metric === 'total.in_bytes' || _metric === 'health.inputs') && input === '${name}'`;
  let msgAlarm = `'Source ${name} traffic volume greater than ${dataVolume} threshold in ${timeWindow}'`;
  let msgOk = `'Source ${name} traffic is within ${dataVolume} threshold in ${timeWindow}'`;

  if(__workerGroup) {
    filter = `(${filter}) && __worker_group === '${__workerGroup}'`;
    msgAlarm = `'Source ${name} in group ${__workerGroup} traffic volume greater than ${dataVolume} threshold in ${timeWindow}'`;
    msgOk = `'Source ${name} in group ${__workerGroup} traffic is within ${dataVolume} threshold in ${timeWindow}'`;
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
              dataVolume,
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
                { name: 'timewindow', value: `'${timeWindow}'` },
                { name: 'dataVolume', value: `'${dataVolume}'` },
              ]
            }
          },
          {
            id: 'eval',
            filter: `bytes >= ${dataVolumeBytes}`,
            conf: {
              add : [
                { name: '_raw', value: `${msgAlarm}`},
                { name: 'status', value: `'ALARM'`},
              ]
            }
          },
          {
            id: 'eval',
            filter: `(bytes || 0 ) < ${dataVolumeBytes}`,
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