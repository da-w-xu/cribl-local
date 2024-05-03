exports.name = 'Destination Backpressure Activated';
exports.type = 'metric';
exports.category = 'destinations';

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
  let workerGroupMessage = "";
  let filter = `_metric === 'backpressure.outputs' && output === '${name}'`;

  const add = [
    { name: 'output', value: `'${name}'` },
    { name: 'timewindow', value: `'${timeWindow}'` },
    { name: '_metric', value: "'backpressure.outputs'" },
  ];

  if (__workerGroup) {
    filter = `${filter} && __worker_group === '${__workerGroup}'`;
    workerGroupMessage = ` in group ${__workerGroup}`;
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
                'max(_value).as(backpressure_type)'
              ],
              lagTolerance: '20s',
              idleTimeLimit: '20s',
            }
          },
          {
            id: 'eval',
            conf: {
              add
            }
          },
          {
            id: 'eval',
            filter: 'backpressure_type === 0 || backpressure_type === undefined', // BackpressureStatus.BLOCKING
            conf: {
              add: [
                { name: 'status', value: "'DISENGAGED'" }, // BackpressureStatus.NONE
                { name: '_raw', value: `'Backpressure is disengaged for destination ${name}${workerGroupMessage}'`}
              ]
            }
          },
          {
            id: 'eval',
            filter: 'backpressure_type === 1', // BackpressureStatus.BLOCKING
            conf: {
              add: [
                { name: 'status', value: "'BLOCKING'" },
                { name: '_raw', value: `'Backpressure (blocking) is engaged for destination ${name}${workerGroupMessage}'`}
              ]
            }
          },
          {
            id: 'eval',
            filter: 'backpressure_type === 2', // BackpressureStatus.DROPPING
            conf: {
              add: [
                { name: 'status', value: "'DROPPING'" },
                { name: '_raw', value: `'Backpressure (dropping) is engaged for destination ${name}${workerGroupMessage}'`}
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
            filter: `!(${notifyOnResolution}) && status === 'DISENGAGED'`,
          }
        ]
      }
    }
  };
};
