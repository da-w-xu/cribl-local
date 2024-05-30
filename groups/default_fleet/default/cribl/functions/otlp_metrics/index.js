exports.name = 'OTLP Metrics';
exports.version = '0.3';
exports.disabled = false;
exports.group = 'Formatters';
exports.sync = true;
exports.handleSignals = true;

const cLogger = C.util.getLogger('func:otlp_metrics');
const { OtelMetricsFormatter } = C.internal.otel;

const statsInterval = 60000; // 1 minute

let otelMetricsFormatterConfig = {};
let otlpBatchConfig = {};
let metricsFormatter;
let statsReportInterval;

function resetStats() {
  metricsFormatter?.resetStats();
}

function reportStats() {
  const {
    numGaugeHistogram,
    numHistogramNoCount,
    numSummaryNoCount,
    numDropped,
    numNotMetric,
    numBatches
  } = metricsFormatter.getStats();

  // Report stats, then reset
  cLogger.debug("Dropped events stats", {
    numGaugeHistogram,
    numHistogramNoCount,
    numSummaryNoCount,
    numNotMetric,
    numDropped,
    numBatches
  });
  resetStats();
}

exports.init = (opts) => {
  const conf = (opts || {}).conf || {};

  otelMetricsFormatterConfig = {
    shouldDropNonMetricEvents: conf.dropNonMetricEvents || false,
    resourceAttributePrefixes: conf.resourceAttributePrefixes
 };

  otlpBatchConfig = {
    enableOTLPMetricsBatching: conf.batchOTLPMetrics,
    sendBatchSize: conf.sendBatchSize ?? 8192,
    timeout: conf.timeout ?? 200,
    sendBatchMaxSize: C.util.parseMemoryStringToBytes(`${conf.sendBatchMaxSize ?? 0}KB`),
    metadataKeys: conf.metadataKeys ?? [],
    metadataCardinalityLimit: conf.metadataCardinalityLimit ?? 1000
  };

  if (otlpBatchConfig.metadataKeys.length > 0 && otlpBatchConfig.metadataCardinalityLimit === 0) {
    // Can't have unlimited cardinality
    cLogger.warn("Can't have unlimited cardinality, setting cardinality to 1000");
    otlpBatchConfig.metadataCardinalityLimit = 1000;
  }

  metricsFormatter = new OtelMetricsFormatter(cLogger, otelMetricsFormatterConfig, otlpBatchConfig);

  resetStats();

  clearInterval(statsReportInterval);
  statsReportInterval = setInterval(reportStats, statsInterval);
};

exports.process = (event) => {
  let flushedEvents = [];

  if (event.__signalEvent__) {
    if (otlpBatchConfig.enableOTLPMetricsBatching) {
      flushedEvents = metricsFormatter.output(event.__signalEvent__ === 'final');
    }
    flushedEvents.push(event);
  } else {
    flushedEvents = metricsFormatter.handleEvent(event);
  }

  return flushedEvents.length === 0 ? null : (flushedEvents.length === 1 ? flushedEvents[0] : flushedEvents);
};

exports.unload = () => {
  metricsFormatter = undefined;

  clearInterval(statsReportInterval);
  statsReportInterval = undefined;
}

//// tests only ////
exports.UT_getStats = () => {
  const {
    numGaugeHistogram,
    numHistogramNoCount,
    numSummaryNoCount,
    numDropped,
    numNotMetric,
    numBatches
  } = metricsFormatter.getStats();
  return {
    numGaugeHistogram,
    numHistogramNoCount,
    numSummaryNoCount,
    numDropped,
    numNotMetric,
    numBatches
  };
};
