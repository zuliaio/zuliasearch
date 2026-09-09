package io.zulia.signals.client;

import io.zulia.signals.model.Signal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

/** Throttled log for dropped signals. The first drop is logged with its stack trace, then one summary per interval. */
final class DroppedSignalLog {

	private static final Logger LOG = LoggerFactory.getLogger(DroppedSignalLog.class);
	static final Duration REPORT_INTERVAL = Duration.ofMinutes(1);

	private final Clock clock;
	private long droppedSinceReport;
	private long droppedTotal;
	private long reportsEmitted;
	private Instant lastReport;

	DroppedSignalLog(Clock clock) {
		this.clock = clock;
	}

	synchronized void dropped(Signal signal, Exception cause) {
		droppedSinceReport++;
		droppedTotal++;
		Instant now = Instant.now(clock);
		if (lastReport == null) {
			LOG.warn("Signals store unavailable, dropping signal {} ({} {} by {}). Later drops are summarized every {}", signal.signalId(), signal.app(),
					signal.actionType(), signal.actor().type(), REPORT_INTERVAL, cause);
			setLastReport(now);
		}
		else if (Duration.between(lastReport, now).compareTo(REPORT_INTERVAL) >= 0) {
			LOG.warn("Signals store still unavailable, dropped {} signals since last report ({} total), latest {} {} by {}: {}", droppedSinceReport,
					droppedTotal, signal.app(), signal.actionType(), signal.actor().type(), cause.toString());
			setLastReport(now);
		}
	}

	synchronized void setupFailed(String what, Exception cause) {
		LOG.warn("Signals storage setup failed for {}, signals are dropped until it succeeds, record retries the setup every {}", what,
				SignalsClient.SETUP_RETRY_INTERVAL, cause);
	}

	synchronized long droppedTotal() {
		return droppedTotal;
	}

	synchronized long reportsEmitted() {
		return reportsEmitted;
	}

	private void setLastReport(Instant now) {
		lastReport = now;
		droppedSinceReport = 0;
		reportsEmitted++;
	}
}
