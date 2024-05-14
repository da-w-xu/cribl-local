const { NestedPropertyAccessor } = C.expr;
const { GeoIpDatabase } = C.internal.Lookup;

exports.name = 'GeoIP';
exports.version = '0.2';
exports.disabled = false;
exports.group = 'Standard';

let geoipDb;
let processEventFn = undefined;

exports.init = (opts) => {
  geoipDb = undefined;
  const { file } = opts.conf;
  return Promise.resolve().then(() => {
    if (opts.conf.outFieldMappings != null && Object.keys(opts.conf.outFieldMappings).length > 0) {
      // Search ip-lookup operator makes use of this function as well, but it has slightly different needs than Stream.
      // Instead of having just one "geoip" object with everything hanging off it, Search needs granular control over
      // which of the data fields are added to the event (i.e. maybe only lat & lon).  It also needs to be able to add
      // them as top-level fields, control naming, inject a prefix, etc.
      const inField = new NestedPropertyAccessor(opts.conf.inField || 'ip');
      const outFieldMappings = Object.keys(opts.conf.outFieldMappings).map((outField) => ({
        geoipSource: new NestedPropertyAccessor(opts.conf.outFieldMappings[outField]),
        eventTarget: new NestedPropertyAccessor(outField)
      }));
      processEventFn = async (event) => {
        const ip = inField.evalOn(event);
        if (ip) {
          const geoip = geoipDb.get(ip);
          if (geoip) {
            for (const mapping of outFieldMappings) {
              mapping.eventTarget.set(event, mapping.geoipSource.get(geoip));
            }
          }
        }
        return event;
      }
    } else {
      // Stream behavior
      const inFields = [new NestedPropertyAccessor(opts.conf.inField || 'ip')];
      const outFields = [new NestedPropertyAccessor(opts.conf.outField || 'geoip')];
      const extraFields = opts.conf.additionalFields || [];
      for (let i = 0; i < extraFields.length; i++) {
        const extraField = extraFields[i];
        const eInField = extraField.extraInField;
        const eOutField = extraField.extraOutField;
        if (eInField && eOutField) {
          inFields.push(new NestedPropertyAccessor(eInField));
          outFields.push(new NestedPropertyAccessor(eOutField));
        }
      }
      processEventFn = async (event) => {
        for (let i = 0; i < inFields.length; i++) {
          const ip = inFields[i].evalOn(event);
          if (ip) outFields[i].set(event, geoipDb.get(ip));
        }
        return event;
      }
    }

    const gDb = GeoIpDatabase.open(file);
    return gDb.ready()
      .then(() => { geoipDb = gDb; });
  });
};

exports.process = (event) => {
  if (!geoipDb) return event;
  return geoipDb.ready().then(() => processEventFn(event));
};

exports.unload = () => {
  geoipDb && geoipDb.close();
  geoipDb = undefined;
  processEventFn = undefined;
}
