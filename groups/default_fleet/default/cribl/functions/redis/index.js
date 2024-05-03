exports.disabled = 0;
exports.name = 'Redis';
exports.group = 'Standard';
exports.version = 0.2;

let client;

exports.unload = () => {
  return client.unload();
}

exports.init = async (opt) => {
  const conf = (opt || {}).conf || {};
  const { Redis } = C.internal;
  client = new Redis();
  await client.init(conf);
  // Maintain backward compatibility with 4.5.1
  // connect is not present on the event processor class but is present on
  // the RedisClient class which was formerly returned in 4.5.1
  client.connect?.()?.catch(()=>{});
};

exports.process = (event) => {
  return client.process(event);
};

exports.UT_getClient = () => client.UT_getClient();
exports.UT_setWaitForReconnect = (val) => {client.UT_setWaitForReconnect(val)}
exports.UT_getWaitForReconnectTimeout = () => {return client.UT_getWaitForReconnectTimeout();}
