exports.name = 'OTLP Metrics';
exports.version = '0.2';
exports.disabled = false;
exports.group = 'Formatters';
exports.sync = true;

const cLogger = C.util.getLogger('func:otlp_metrics');
const { OtelMetricsFormatter } = C.internal.otel;

const dropMetricsMap = new Map([['info', 'target']]);

const statsInterval = 60000; // 1 minute

let numDropped, numLocalNotMetric;
let metricsFormatter;
let statsReportInterval;
let shouldDropNonMetricEvents;

function resetStats() {
  metricsFormatter?.resetStats();
  numDropped = 0;
  numLocalNotMetric = 0;
}

function reportStats() {
  const {
    numGaugeHistogram,
    numHistogramNoCount,
    numSummaryNoCount,
    numNotMetric
  } = metricsFormatter.getStats();

  // Report stats, then reset
  cLogger.debug("Dropped events stats", {
    numGaugeHistogram,
    numHistogramNoCount,
    numSummaryNoCount,
    numNotMetric: numNotMetric + numLocalNotMetric,
    numDropped
  });
  resetStats();
}

exports.init = (opts) => {
  const conf = (opts || {}).conf || {};
  shouldDropNonMetricEvents = conf.dropNonMetricEvents || false;

  metricsFormatter = new OtelMetricsFormatter(cLogger, conf.resourceAttributePrefixes);

  resetStats();

  clearInterval(statsReportInterval);
  statsReportInterval = setInterval(reportStats, statsInterval);
};

exports.process = (event) => {
  // Cribl metrics

  // Short circuit and return early if event is not a metric event
  if (!('__criblMetrics' in event)) {
    numLocalNotMetric++;
    if (shouldDropNonMetricEvents) {
      numDropped++;
      return null;
    } else {
      return event;
    }
  }

  const metricEvent = metricsFormatter?.formatEvent(event);
  if (metricEvent == null) {
    if (shouldDropNonMetricEvents || (event._metric && dropMetricsMap.get(event._metric_type) === event._metric)) {
      numDropped++;
      return null;
    } else {
      return event;
    }
  }

  return metricEvent;
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
    numNotMetric
  } = metricsFormatter.getStats();
  return {
    numGaugeHistogram,
    numHistogramNoCount,
    numSummaryNoCount,
    numDropped,
    numNotMetric: numNotMetric + numLocalNotMetric
  };
};
