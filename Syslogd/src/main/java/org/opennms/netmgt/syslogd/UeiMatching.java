package org.opennms.netmgt.syslogd;

import static org.opennms.core.utils.InetAddressUtils.str;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.opennms.netmgt.config.SyslogdConfig;
import org.opennms.netmgt.config.SyslogdConfigFactory;
import org.opennms.netmgt.config.syslogd.HideMatch;
import org.opennms.netmgt.config.syslogd.HostaddrMatch;
import org.opennms.netmgt.config.syslogd.HostnameMatch;
import org.opennms.netmgt.config.syslogd.ParameterAssignment;
import org.opennms.netmgt.config.syslogd.ProcessMatch;
import org.opennms.netmgt.config.syslogd.UeiMatch;
import org.opennms.netmgt.model.events.EventBuilder;
import org.opennms.netmgt.eventd.Runner;
import org.opennms.netmgt.syslogd.api.SyslogMessageDTO;
import org.opennms.netmgt.syslogd.api.SyslogMessageLogDTO;
import org.opennms.netmgt.syslogd.api.UtilMarshler;
import org.opennms.netmgt.xml.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.eventbus.Message;

public class UeiMatching extends AbstractVerticle {

	private static SyslogdConfigFactory syslogdConfig;

	private static Event m_event;

	private ExecutorService backgroundConsumer;

	private static List<UeiMatch> ueiMatch;

	private static List<HideMatch> hideMatch;

	private static UtilMarshler utilMarshler;

	private static final Logger LOG = LoggerFactory.getLogger(UeiMatching.class);

	private Pattern msgPat = null;
	private Matcher msgMat = null;

	private static final LoadingCache<String, Pattern> CACHED_PATTERNS = CacheBuilder.newBuilder()
			.build(new CacheLoader<String, Pattern>() {
				public Pattern load(String expression) {
					try {
						return Pattern.compile(expression, Pattern.MULTILINE);
					} catch (final PatternSyntaxException e) {
						LOG.warn("Failed to compile regex pattern '{}'", expression, e);
						return null;
					}
				}
			});

	public static void main(String[] args) throws IOException {
		utilMarshler = new UtilMarshler(SyslogMessageLogDTO.class);
		syslogdConfig = ConvertToEvent.loadSyslogConfiguration("/syslogd-loadtest-configuration.xml");
		ueiMatch = (syslogdConfig.getUeiList() == null ? Collections.emptyList() : syslogdConfig.getUeiList());
		hideMatch = (syslogdConfig.getHideMessages() == null ? Collections.emptyList()
				: syslogdConfig.getHideMessages());
		DeploymentOptions deployment = new DeploymentOptions();
		deployment.setWorker(true);
		deployment.setWorkerPoolSize(Integer.MAX_VALUE);
		deployment.setMultiThreaded(true);
		// deployment.setInstances(100);
		Runner.runClusteredExample(UeiMatching.class, deployment);

	}

	@Override
	public void start() throws Exception {
		backgroundConsumer = Executors.newSingleThreadExecutor();
		backgroundConsumer.submit(() -> {
			io.vertx.core.eventbus.MessageConsumer<String> consumerFromEventBus = vertx.eventBus().consumer("uei");
			consumerFromEventBus.handler(syslogDTOMessage -> {

				try {
					testFix((SyslogMessageLogDTO) utilMarshler.unmarshal(syslogDTOMessage.body()));
					System.out.println("At Uei " + SyslogTimeStamp.broadcastCount.incrementAndGet());
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			});
		});
	}

	private void testFix(SyslogMessageLogDTO syslogMessage) throws MessageDiscardedException {

		SyslogMessage message = syslogMessage.getSyslogMessage();
		final String priorityTxt = message.getSeverity().toString();
		final String facilityTxt = message.getFacility().toString();
		EventBuilder bldr = syslogMessage.getEventBuilder();
		final String fullText = message.getFullText();
		for (final UeiMatch uei : ueiMatch) {
			final boolean messageMatchesUeiListEntry = containsIgnoreCase(uei.getFacilities(), facilityTxt)
					&& containsIgnoreCase(uei.getSeverities(), priorityTxt)
					&& matchProcess(uei.getProcessMatch().orElse(null), message.getProcessName())
					&& matchHostname(uei.getHostnameMatch().orElse(null), message.getHostName())
					&& matchHostAddr(uei.getHostaddrMatch().orElse(null), str(message.getHostAddress()));

			if (messageMatchesUeiListEntry) {
				if (uei.getMatch().getType().equals("substr")) {
					if (matchSubstring(message.getMessage(), uei, bldr, syslogdConfig.getDiscardUei())) {
						break;
					}
				} else if ((uei.getMatch().getType().startsWith("regex"))) {
					if (matchRegex(message.getMessage(), uei, bldr, syslogdConfig.getDiscardUei())) {
						break;
					}
				}
			}
		}

		// Time to verify if we need to hide the message
		boolean doHide = false;
		if (hideMatch.size() > 0) {
			for (final HideMatch hide : hideMatch) {
				if (hide.getMatch().getType().equals("substr")) {
					if (fullText.contains(hide.getMatch().getExpression())) {
						// We should hide the message based on this match
						doHide = true;
						break;
					}
				} else if (hide.getMatch().getType().equals("regex")) {
					try {
						msgPat = getPattern(hide.getMatch().getExpression());
						msgMat = msgPat.matcher(fullText);
						if (msgMat.find()) {
							// We should hide the message based on this match
							doHide = true;
							break;
						}
					} catch (PatternSyntaxException pse) {
						LOG.warn("Failed to compile hide-match regex pattern '{}'", hide.getMatch().getExpression(),
								pse);
					}
				}
			}
		}

	}

	private static boolean matchFind(final String expression, final String input, final String context) {
		final Pattern pat = getPattern(expression);
		if (pat == null) {
			LOG.debug("Unable to get pattern for expression '{}' in {} context", expression, context);
			return false;
		}
		final Matcher mat = pat.matcher(input);
		if (mat != null && mat.find())
			return true;
		return false;
	}

	private static boolean matchHostAddr(final HostaddrMatch hostaddrMatch, final String hostAddress) {
		if (hostaddrMatch == null)
			return true;
		if (hostAddress == null)
			return false;

		final String expression = hostaddrMatch.getExpression();

		if (matchFind(expression, hostAddress, "hostaddr-match")) {
			LOG.trace("Successful regex hostaddr-match for input '{}' against expression '{}'", hostAddress,
					expression);
			return true;
		}
		return false;
	}

	private static boolean matchHostname(final HostnameMatch hostnameMatch, final String hostName) {
		if (hostnameMatch == null)
			return true;
		if (hostName == null)
			return false;

		final String expression = hostnameMatch.getExpression();

		if (matchFind(expression, hostName, "hostname-match")) {
			LOG.trace("Successful regex hostname-match for input '{}' against expression '{}'", hostName, expression);
			return true;
		}
		return false;
	}

	private static boolean matchProcess(final ProcessMatch processMatch, final String processName) {
		if (processMatch == null)
			return true;
		if (processName == null)
			return false;

		final String expression = processMatch.getExpression();

		if (matchFind(expression, processName, "process-match")) {
			LOG.trace("Successful regex process-match for input '{}' against expression '{}'", processName, expression);
			return true;
		}
		return false;
	}

	private static boolean containsIgnoreCase(List<String> collection, String match) {
		if (collection.size() == 0)
			return true;
		for (String string : collection) {
			if (string.equalsIgnoreCase(match))
				return true;
		}
		return false;
	}

	private static Pattern getPattern(final String expression) {
		return CACHED_PATTERNS.getUnchecked(expression);
	}

	/**
	 * Checks the message for substring matches to a {@link UeiMatch}. If the
	 * message matches, then the UEI is updated (or the event is discarded if the
	 * discard UEI is used). Parameter assignments are NOT performed for substring
	 * matches.
	 * 
	 * @param message
	 * @param uei
	 * @param bldr
	 * @param discardUei
	 * @return
	 * @throws MessageDiscardedException
	 */
	private static boolean matchSubstring(String message, final UeiMatch uei, final EventBuilder bldr,
			final String discardUei) throws MessageDiscardedException {
		final boolean traceEnabled = LOG.isTraceEnabled();
		if (message.contains(uei.getMatch().getExpression())) {
			if (discardUei.equals(uei.getUei())) {
				if (traceEnabled)
					LOG.trace("Specified UEI '{}' is same as discard-uei, discarding this message.", uei.getUei());
				throw new MessageDiscardedException();
			} else {
				// Update the UEI to the new value
				if (traceEnabled)
					LOG.trace("Changed the UEI of a Syslogd event, based on substring match, to : {}", uei.getUei());
				bldr.setUei(uei.getUei());
				return true;
			}
		} else {
			if (traceEnabled)
				LOG.trace("No substring match for text of a Syslogd event to : {}", uei.getMatch().getExpression());
			return false;
		}
	}

	/**
	 * Checks the message for matches to a {@link UeiMatch}. If the message matches,
	 * then the UEI is updated (or the event is discarded if the discard UEI is
	 * used) and parameters are added to the event.
	 * 
	 * @param message
	 * @param uei
	 * @param bldr
	 * @param discardUei
	 * @return
	 * @throws MessageDiscardedException
	 */
	private static boolean matchRegex(final String message, final UeiMatch uei, final EventBuilder bldr,
			final String discardUei) throws MessageDiscardedException {
		final boolean traceEnabled = LOG.isTraceEnabled();
		final String expression = uei.getMatch().getExpression();
		final Pattern msgPat = getPattern(expression);
		if (msgPat == null) {
			LOG.debug("Unable to create pattern for expression '{}'", expression);
			return false;
		}

		final Matcher msgMat = msgPat.matcher(message);

		// If the message matches the regex
		if ((msgMat != null) && (msgMat.find())) {
			// Discard the message if the UEI is set to the discard UEI
			if (discardUei.equals(uei.getUei())) {
				if (traceEnabled)
					LOG.trace("Specified UEI '{}' is same as discard-uei, discarding this message.", uei.getUei());
				throw new MessageDiscardedException();
			} else {
				// Update the UEI to the new value
				if (traceEnabled)
					LOG.trace("Changed the UEI of a Syslogd event, based on regex match, to : {}", uei.getUei());
				bldr.setUei(uei.getUei());
			}

			if (msgMat.groupCount() > 0) {
				// Perform default parameter mapping
				if (uei.getMatch().getDefaultParameterMapping()) {
					if (traceEnabled)
						LOG.trace("Doing default parameter mappings for this regex match.");
					for (int groupNum = 1; groupNum <= msgMat.groupCount(); groupNum++) {
						if (traceEnabled)
							LOG.trace(
									"Added parm 'group{}' with value '{}' to Syslogd event based on regex match group",
									groupNum, msgMat.group(groupNum));
						bldr.addParam("group" + groupNum, msgMat.group(groupNum));
					}
				}

				// If there are specific parameter mappings as well, perform those mappings
				if (uei.getParameterAssignments().size() > 0) {
					if (traceEnabled)
						LOG.trace("Doing user-specified parameter assignments for this regex match.");
					for (ParameterAssignment assignment : uei.getParameterAssignments()) {
						String parmName = assignment.getParameterName();
						String parmValue = msgMat.group(assignment.getMatchingGroup());
						parmValue = parmValue == null ? "" : parmValue;
						bldr.addParam(parmName, parmValue);
						if (traceEnabled) {
							LOG.trace(
									"Added parm '{}' with value '{}' to Syslogd event based on user-specified parameter assignment",
									parmName, parmValue);
						}
					}
				}
			}

			return true;
		}

		if (traceEnabled)
			LOG.trace("Message portion '{}' did not regex-match pattern '{}'", message, expression);
		return false;
	}

}
