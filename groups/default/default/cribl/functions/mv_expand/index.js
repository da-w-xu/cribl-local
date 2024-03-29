exports.name = 'mv_expand';
exports.version = '0.1';
exports.handleSignals = false;
exports.group = C.INTERNAL_FUNCTION_GROUP;
exports.sync = true;

const { NestedPropertyAccessor } = C.expr;
const cLogger = C.util.getLogger('func:mv_expand');

let sourceFields; // array of npa
let targetNames; // array of npa
let rowLimit; // 1..MaxSafeInt
let itemIndexName; // npa
let arrayBagMode; // boolean

exports.init = (opts) => {
  const conf = opts?.conf;
  let gotTargetNames = false;

  sourceFields = [];
  targetNames = [];

  if (!conf) {
    throw new Error('Multi-value Expand not supported w/o configuration');
  }

  if (!conf.sourceFields) {
    throw new Error('sourceFields missing on multi-value expand configuration');
  }

  if (!Array.isArray(conf.sourceFields) || !conf.sourceFields.length) {
    throw new Error('Expected array of property names on sourceFields configuration');
  } 

  if (conf.targetNames) {
    if (!Array.isArray(conf.targetNames) || conf.targetNames.length !== conf.sourceFields.length) {
      throw new Error(`targetNames configuration isn't matching the number of sourceFields entries`);
    }

    gotTargetNames = true;
    for (const tn of conf.targetNames) {
      targetNames.push(new NestedPropertyAccessor(tn, cLogger));
    }
  } 

  for (const sf of conf.sourceFields) {
    const npa = new NestedPropertyAccessor(sf, cLogger);
    sourceFields.push(npa);
    if (!gotTargetNames) {
      targetNames.push(npa);
    }
  }

  if (conf.rowLimit == null) {
    rowLimit = Number.MAX_SAFE_INTEGER;
  } else {
    rowLimit = conf.rowLimit;

    if (rowLimit < 1 || rowLimit > Number.MAX_SAFE_INTEGER || Math.floor(rowLimit) !== rowLimit) {
      throw new Error('invalid value for rowLimit in mv-expand configuration');
    }
  }

  if (conf.itemIndexName) {
    itemIndexName = new NestedPropertyAccessor(conf.itemIndexName, cLogger);
  }

  arrayBagMode = conf.bagExpansionMode === 'array';
};

exports.unload = () => {
  sourceFields = undefined;
  targetNames = undefined;
  itemIndexName = undefined;
}

exports.process = (event) => {
  let outputEvents = [event];

  // iterate all fields in the event that should be expanded
  for (let expandIdx = 0; expandIdx < sourceFields.length; ++expandIdx) {
    const sf = sourceFields[expandIdx];
    const tn = targetNames[expandIdx];
    const iterationEvents = [];

    // handle each (intermittent) created event
    for (const oe of outputEvents) {
      const field = sf.get(oe); // field to expand
      if (field != null) {
        if (Array.isArray(field)) {
          for (let fieldIdx = 0; fieldIdx < field.length; ++fieldIdx) {
            const val = field[fieldIdx];
            const expandedEvent = oe.__clone(true);

            tn.set(expandedEvent, val);
            iterationEvents.push(expandedEvent);
          }
        } else if (typeof field === 'object') {
          const pairs = Object.entries(field);
          for (let entryIdx = 0; entryIdx < pairs.length; ++entryIdx) {
            const pair = pairs[entryIdx];
            const expandedEvent = oe.__clone(true);

            if (arrayBagMode) {
              // newField = [key, value]
              tn.set(expandedEvent, pair);
            } else {
              // newField = { key: value }
              const newBag = {};
              newBag[pair[0]] = pair[1];
              tn.set(expandedEvent, newBag);
            }            

            iterationEvents.push(expandedEvent);
          }
        } else {
          // neither array nor object to expand here
          iterationEvents.push(oe);
        }
      } else {
        // no field found to expand
        iterationEvents.push(oe);
      }
    }

    if (iterationEvents.length >= rowLimit) {
      outputEvents = iterationEvents.slice(0, rowLimit);
      break;
    } else {
      outputEvents = iterationEvents;
    }
  }

  // optional: inject an index into the output events
  if (itemIndexName) {
    for (let idx = 0; idx < outputEvents.length; ++idx) {
      itemIndexName.set(outputEvents[idx], idx);
    }
  }

  return outputEvents;
}  
