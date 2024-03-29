exports.name = 'Eval';
exports.version = '0.4';
exports.disabled = false;
exports.group = 'Standard';
exports.sync = true;

const { Expression, NestedPropertyAccessor } = C.expr;
const cLogger = C.util.getLogger('func:eval');

// The write NPA for indirect addressing are create based in field values
// which means that the amount can grow across processed events. To prevent
// uncontrolled growth of the NPA cache for indirect fields, we empty the
// cache as soon as it reaches this size.
const WRITE_ACCESSOR_MAP_MAX_SIZE = 2048;

const CTRL_PREFIX = '_ctrl.';
const E_NOTATION_REGEXP = /__e\[__e\[("(.+)")|('(.+)')\]\]/;

let fields2add = []; // key1, expr1, key2, expr2 ...
let fields2remove = []; // list of fields to remove
let WL2remove = null; // wildcarded fields to remove
let WL2keep = null;
let writeAccessorMap = null; // used to cache NPA instances for indirect addressing

function getAccessor(fieldName) {
  if (fieldName) {
    // if the field name is given as indirect addressing like __e[nameVar] where
    // nameVar is an event property that contains the real target field name, we
    // create this wrapper around NestedPropertyAccessor, which first reads the
    // value of nameVar and then writes the content to that property name
    const e_match = fieldName.match(E_NOTATION_REGEXP);
    if (e_match) {
      fieldName = e_match[2] ?? e_match[4];

      // use this as cache for the NPA, created for the specific field value
      if (writeAccessorMap == null) {
        writeAccessorMap = new Map();
      }

      cLogger.debug('creating indirect addressing', {indirectName: fieldName});
      return {
        readAccessor: new NestedPropertyAccessor(fieldName),
        set: function(event, value) {
          const targetFieldName = this.readAccessor.get(event);
          if (targetFieldName != null) {
            let targetAccessor = writeAccessorMap.get(targetFieldName);
            if (targetAccessor == null) {
              // simple limitation of write NPA size (clear cache if > limit)
              if (writeAccessorMap.size >= WRITE_ACCESSOR_MAP_MAX_SIZE) {
                cLogger.debug('clearing out NPA cache for indirect addressing', {cacheSize: writeAccessorMap.size});
                writeAccessorMap = new Map();
              }

              targetAccessor = new NestedPropertyAccessor(targetFieldName);
              writeAccessorMap.set(targetFieldName, targetAccessor);
            }
            targetAccessor.set(event, value);
          }
        }
      }
    }
    
    // normal field name, use NestedPropertyAccessor directly
    return new NestedPropertyAccessor(fieldName);
  }
  return fieldName;
}

exports.init = (opts) => {
  const conf = opts.conf;
  fields2add = [];
  fields2remove = [];
  WL2remove = null;
  WL2keep = null;

  const add = [];
  const remove = [];
  (conf.add || []).forEach(field => {
    if (field.disabled) return; // Ignore disabled fields
    field.name = (field.name || '').trim();
    const isCtrlField = field.name.startsWith(CTRL_PREFIX);
    add.push(isCtrlField);
    add.push(isCtrlField ? field.name.substr(CTRL_PREFIX.length) : getAccessor(field.name));
    add.push(new Expression(`${field.value}`, { disallowAssign: true }));
  });

  const removePatterns = [];
  (conf.remove || []).forEach(field => {
    field = (field || '').trim();
    if (field.indexOf('*') > -1) {
      removePatterns.push(field);
    } else {
      remove.push(field);
    }
  });

  const keepPatterns = (conf.keep || []).map(k => k.trim()).filter(k => k.length);
  if (keepPatterns.length > 0) {
    WL2keep = new C.util.WildcardList(keepPatterns);
  }

  if (removePatterns.length > 0) {
    WL2remove = new C.util.WildcardList(removePatterns, keepPatterns);
  }

  fields2add = add;
  fields2remove = remove.filter(field => (!WL2keep || !WL2keep.test(field))).map(getAccessor);
};

exports.process = (event) => {
  if(!event) return event;
  // add/replace some fields
  for (let i = 2; i < fields2add.length; i += 3) {
    const key = fields2add[i - 1];
    const val = fields2add[i].evalOn(event);
    if (!fields2add[i - 2]) {
      // might need to throw away the result
      if (key) key.set(event, val);
    } else {
      event.__setCtrlField(key, val);
    }
  }
  // remove some fields, here we simply set fields to undefined for performance reasons
  for (let i = 0; i < fields2remove.length; i++) {
    fields2remove[i].set(event, undefined);
  }

  // remove wildcard fields
  if (WL2remove) {
    event.__traverseAndUpdate(5, (path, value) => {
      return WL2remove.test(path) ? undefined : value;
    });
  }
  return event;
};
