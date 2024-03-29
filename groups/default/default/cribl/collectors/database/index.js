/* eslint-disable no-await-in-loop */

exports.name = 'Database';
exports.version = '0.1';
exports.disabled = false;
exports.destroyable = false;

const stream = require('stream');

let conf;
let queryExpression;

const isSelect = (query) => {
  const queries = query.split(';');
  if (queries.length > 2 || (queries.length === 2 && queries[1].trim() !== '')) {
    return false;
  }
  return query.trim().toLowerCase().startsWith('select');
}

/**
 * Intercepts database rows before they are transformed into JSON, and uses the row data to update and report collector 
 * state. This transform is only used if state tracking is enabled for a given job.
 */
class StateReportingTransform extends stream.Transform {
  constructor(options, job, state) {
    super({
      objectMode: true,
      ...options
    });
    this.job = job;
    this.state = state;
  }

  _transform(chunk, encoding, callback) {
    if (!Object.hasOwn(chunk, conf.stateTracking.trackingColumn)) {
      this.destroy(`Tracking column "${conf.stateTracking.trackingColumn}" was missing from row`);
      return;
    }

    const rowValue = chunk[conf.stateTracking.trackingColumn];
    if (typeof rowValue !== 'number' && typeof rowValue !== 'string') {
      this.job.logger().error('Invalid row value for tracking column, must be a number or string', {
        valueType: typeof rowValue,
        column: conf.stateTracking.trackingColumn
      });

      this.destroy(`Tracking column "${conf.stateTracking.trackingColumn}" contained non-number/non-string value`);
      return;
    }

    this.state[conf.stateTracking.trackingColumn].value = rowValue;
    this.job.reportState(this.state);

    this.push(chunk);
    callback();
  }

  _flush(callback) {
    this.job.logger().debug('State after processing results', { state: this.state });
    callback();
  }
}

exports.metricDims = () => {
  const connection = conf.connectionId;
  const databaseConnectionsMgr = C.internal.DatabaseConnectionsMgr.instance();
  return {databaseType: databaseConnectionsMgr.getDatabaseType(connection)};
};

exports.UT_getAllMetricDims = () => {
  const databaseConnectionsMgr = C.internal.DatabaseConnectionsMgr.instance();
  const allDims = [];
  for (const databaseType of databaseConnectionsMgr.getDatabaseTypes()) {
    allDims.push({ databaseType });
  }
  return allDims;
};

exports.init = async (opts) => {
  const { Expression } = C.expr;

  conf = opts.conf || {};

  queryExpression = new Expression(conf.query, {disallowAssign: true});
  if ((conf.queryValidationEnabled ?? true) && !isSelect(queryExpression.evalOn
({}))) {
    throw new Error('Invalid config - Must provide only a single SELECT query');
  }
};

exports.discover = async (job) => {
  await job.addResult({
    format: 'raw',
    stateKey: 'databaseCollectorState',
  });
};

exports.collect = async (collectible, job) => {
  const connection = conf.connectionId;
  const databaseConnectionsMgr = C.internal.DatabaseConnectionsMgr.instance();
  let client;
  try {
    client = databaseConnectionsMgr.getConnection(connection, job.logger());
  } catch (err) {
    job.reportError(err);
  }

  if (client == null) {
    throw new Error(`Can't find Database Connection '${connection}'`);
  }

  let state = collectible.state;

  // We get both the prepared statement itself as well as the list of variables referenced in the evaluated version of 
  // the expression. This is important, since some DBs (e.g. Postgres) require that the arguments passed into 
  // executeQuery() match exactly the number and order of arguments in the prepared statement. This cannot be known by 
  // inspecting the expression itself at this point, for two reasons:
  // 
  // 1. queryExpression.references() will only pick up simple variables - if the expression references something like 
  //    ${state.foo.value} to pick up the value of the rising column, then the expression library will tell us that 
  //    `state`, `foo`, and `value` are all referenced, but not `state.foo.value`.
  // 2. queryExpression.references() will tell us if the *expression* references the variable, but that's not the same
  //    as whether the *query* resulting from the expression references the variable, and that's what we need to know
  //    below since that governs whether or not we need to add a value for the variable to the args object.
  // 
  // It's expected that on the first run of a job, we won't have state (no previous runs, right?) and therefore the 
  // query that we run will need to NOT have the rising column in its WHERE clause. To accomplish this, the recommended
  // approach is to embed rising columns in a ternary statement, such as:
  //
  //    `SELECT * FROM foo WHERE ${state && state.id ? "id > " + state.id.value : "1=1"}`
  // 
  // On the first run, this query is simply 'SELECT * FROM foo WHERE 1=1' but on subsequent runs, it'll be 
  // 'SELECT * FROM foo WHERE id > $1' (or however prepared statements look for the relevant database).
  const preparedStatementData = client.prepareStatement(queryExpression, state);

  if (preparedStatementData) {
    const args = {};
    try {
      const preparedStatement = preparedStatementData.preparedStatement;
      const referencedVariables = preparedStatementData.referencedVariables;

      // If the database uses positional arguments for prepared statements, the order of referencedVariables dictates
      // the order the values for those arguments must be placed in args, so the right value goes to the right argument.
      for (const referencedVariable of referencedVariables) {
        if (referencedVariable === 'earliest') {
          args['earliest'] = new Date(isNaN(+conf.earliest) ? Date.now() : conf.earliest * 1000).toISOString();
        } else if (referencedVariable === 'latest') {
          args['latest'] = new Date(isNaN(+conf.latest) ? Date.now() : conf.latest * 1000).toISOString();
        } else {
          args[referencedVariable] = state[referencedVariable].value;
        }
      }
      job.logger().debug('Query', { preparedStatement, args });

      let query = await client.executeQuery(preparedStatement, state, args);
      if (conf.stateTracking?.enabled) {
        // If state has never been reported for this collector, state will be undefined, so we'll need to create it.
        if (!state) {
          state = { [conf.stateTracking.trackingColumn]: { value: null } };
        }
        job.logger().debug('State before processing results', {state});
        const transform = new StateReportingTransform({}, job, state);
        query = stream.pipeline(query, transform, () => {});
      }

      const readable = await C.internal.stringifyReadableObjects(query);
      return readable;
    } catch (err) {
      job.reportError(new Error('Collect error', { err })).catch(() => {});
      throw err;
    }
  } else {
    job.reportError(new Error('Unable to prepare statement', { queryExpression })).catch(() => {});
  }
};
