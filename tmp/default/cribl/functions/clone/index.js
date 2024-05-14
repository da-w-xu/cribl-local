exports.name = 'Clone';
exports.version = '0.2';
exports.group = 'Advanced';
exports.sync = true;
const { NestedPropertyAccessor, Expression } = C.expr;

const cLogger = C.util.getLogger('func:clone');

let clones = [];
let cloneKeys = [];
let cloneVals = [];

exports.init = (opts) => {
  const conf = opts.conf || {};
  clones = conf.clones || [];
  cloneKeys = clones.map(c => Object.keys(c).map(k => new NestedPropertyAccessor(k)));
  cloneVals = clones.map(c => Object.keys(c).map(k => buildValueFunction(c[k])));
};

// This is essentially runExprSafe, but allowing us to avoid constantly rebuilding the Expression object for performance 
// reasons, especially in cases where it doesn't resolve to a valid expression
function buildValueFunction(value) {
  try {
    // Pre-build the expression for reuse between evaluations
    const expression = new Expression(value);
    return (e) => {
      try {
        const result = expression.evalOn(e);
        return (result == null || Number.isNaN(result)) ? value : result;
      } catch {
        return value
      }
    }
  } catch {
    // If we can't parse an expression, assume this is a non-expression constant
    // The value function just needs to return that constant value
    cLogger.warn(`Parsing of value expression ('${value}') failed.  Interpreting as a string literal.`);
    return (e) => value
  }
}


exports.process = (event) => {
  if (clones.length === 0) {
    return event;
  }
  const result = new Array(clones.length + 1);
  result[0] = event;
  const eventKeys = Object.keys(event);
  for (let i = 0; i < clones.length; i++) {
    const keys = cloneKeys[i];
    const vals = cloneVals[i];
    const copy = event.__clone(false, eventKeys);
    for (let k = 0; k < keys.length; k++) {
      keys[k].set(copy, vals[k](event));
    }
    result[i + 1] = copy;
  }
  return result;
};
