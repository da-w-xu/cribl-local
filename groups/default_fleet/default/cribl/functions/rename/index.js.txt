exports.name = 'Rename';
exports.version = '0.1';
exports.disabled = false;
exports.group = 'Standard';
exports.sync = true;

const { Expression, NestedPropertyAccessor } = C.expr;
const criblInternalField = C.internal.criblInternalField;
const cLogger = C.util.getLogger('func:rename');

const DEFAULT_DEPTH = 5;

let fields2rename = []; //NestedPropertyAccessor[]: currentName1, newName1, currentName2, newName2 ... 
let renameExpr;
let baseFields = []; //Simple base fields
let baseWCFields = null; // Base fields with wildcards
let wildcardDepth;
let internalBaseFieldsRemoved = false


function getAccessor(fieldName) {
    return new NestedPropertyAccessor(fieldName);
}

function rename(currentField, newField, context) {
  if (typeof currentField !== 'object' || typeof newField !== 'object') return;
  const val = currentField.get(context);
  // There's no reason to proceed if the source is already undefined
  if (val === undefined) return;
  // rename is just about creating a new field with the new name
  newField.set(context, currentField.get(context));
  // for performance consideration, we set the old field to undefined 
  // instead of deleting it
  currentField.set(context, undefined);
}

function parseExpr (expr) {
  if (!expr) return undefined;
  return new Expression(`${expr}`, { disallowAssign: true, args: ['name', 'value'] });
}

function renameFieldsOn(base, event) {
  if (base == null || typeof base !== 'object') return;

  // rename by key-value
  for (let i = 1; i < fields2rename.length; i += 2) {
    rename(fields2rename[i - 1], fields2rename[i], base);
  }

  // rename by expression
  if (renameExpr) {
    for (const [name, value] of Object.entries(base)) {
      if (!criblInternalField(name)) {
        const newName = renameExpr.evalOn(event, name, value);
        if (newName != null && name !== newName && !criblInternalField(newName)) {
          base[newName] = base[name];
          base[name] = undefined;
        }
      }
    }
  }
}

// strip quotes from field names since they're valid in the configuration,
//  then check if internal field. Only needed for init, where we're not worried about hotpath perf
const isInternalField = (field) => {
  return criblInternalField(field.replace(/^["']/, ''));
};

exports.init = (opts) => {
  const conf = opts.conf;
  fields2rename = [];
  baseFields = [];
  baseWCFields = null;
  const stagedRename = [];
  const ignoredFieldPairs = [];
  (conf.rename || []).forEach((field) => {
    if (isInternalField(field.currentName) || isInternalField(field.newName)) {
      ignoredFieldPairs.push(`${field.currentName} -> ${field.newName}`);
    } else {
      stagedRename.push(getAccessor((field.currentName || '').trim()));
      stagedRename.push(getAccessor((field.newName || '').trim()));
    }
  });

  if (ignoredFieldPairs.length > 0) {
    cLogger.warn('Ignoring internal fields in rename function', { ignoredFieldPairs });
  }
  fields2rename = stagedRename;

  const simpleBaseFields = [];
  const baseFieldPatterns = [];
  const sourceBaseFields = conf.baseFields || [];

  for (let i = 0; i < sourceBaseFields.length; i++) {
    const field = sourceBaseFields[i].trim();
    if (isInternalField(field)) {
      internalBaseFieldsRemoved = true;
      cLogger.warn(`Ignoring internal field ${field} in rename function parent fields`);
    } else if (field.length > 0) {
      if (field.includes('*')) {
        baseFieldPatterns.push(field);
      } else {
        simpleBaseFields.push(field);
      }
    }
  }

  if (baseFieldPatterns.length > 0) {
    baseWCFields = new C.util.WildcardList([...new Set(baseFieldPatterns)]);
    wildcardDepth = conf.wildcardDepth == null ? DEFAULT_DEPTH : Number(conf.wildcardDepth);
    if (isNaN(wildcardDepth) || wildcardDepth < 0) {
      throw new Error('wildcardDepth must be a positive integer value');
    }
  }
  baseFields = [...new Set(simpleBaseFields)].filter(x => !baseWCFields || !baseWCFields.test(x)).map(getAccessor);
  renameExpr = parseExpr(conf.renameExpr);

};

exports.process = (event) => {

  if (!event) return event;

  if (fields2rename.length === 0 && !renameExpr) return event;

  // rename fields
  if (baseFields.length === 0 && baseWCFields == null && !internalBaseFieldsRemoved) {
    renameFieldsOn(event, event);
  } else {
    for (let y = 0; y < baseFields.length; y++) {
      renameFieldsOn(baseFields[y].get(event), event);
    }
    if (baseWCFields) {
      event.__traverseAndUpdate(wildcardDepth, (path, value) => {
        if (baseWCFields.test(path)) {
          renameFieldsOn(value, event);
        }
        return value;
      });
    }
  }
  return event;
};
