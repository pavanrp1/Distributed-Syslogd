package org.vertx.kafka;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author ms043660
 */
public class EventdSyslogStresser {
	private static InetAddress m_agentAddress;
	private static Double m_syslogRate = Double.valueOf(100); // seconds
	private static Integer m_syslogCount = Integer.valueOf(500);
	private static Integer m_batchDelay = Integer.valueOf(1); // seconds
	private static Integer m_batchSize = m_syslogCount;
	private static int m_batchCount = 1;
	private static Producer<String, String> producer;

	@SuppressWarnings("unused")
	private static long m_sleepMillis = 0;
	private static String topicName;

	private final static String syslogMessage = "<syslog-message-log source-address=\"127.0.0.1\" source-port=\"1514\" system-id=\"99\" location=\"MalaMac\">\n"
			+ "   <messages timestamp=\"" + iso8601OffsetString(new Date(0), ZoneId.systemDefault(), ChronoUnit.SECONDS)
			+ "\">PDMxPm1haW46IDIwMTctMTAtMDMgbG9jYWxob3N0IGZvbyVkOiBsb2FkIHRlc3RwYXZhbiAlZCBvbiB0dHkx</messages>\n"
			+ "</syslog-message-log>";


	public static void main(String[] args) throws UnknownHostException {

		m_syslogRate = Double.valueOf(args[0]);
		m_syslogCount = Integer.valueOf(args[1]);
		m_agentAddress = InetAddrUtils.addr(args[2]);
		m_batchCount = Integer.valueOf(args[3]);

		m_batchSize = m_syslogCount;

		System.out.println("m_syslogRate : " + m_syslogRate);
		System.out.println("m_syslogCount : " + m_syslogCount);
		System.out.println("m_agentAddress : " + m_agentAddress);
		System.out.println("Commencing the Eventd Stress Test...");
		executeStressTest();

	}

	private static void executeStressTest() {
		try {
			stressEventd();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public static void stressEventd()
			throws ClassNotFoundException, SQLException, IllegalStateException, InterruptedException {

		if (m_batchCount < 1) {
			throw new IllegalArgumentException("Batch count of < 1 is not allowed.");
		} else if (m_batchCount > m_syslogCount) {
			throw new IllegalArgumentException("Batch count is > than syslog count.");
		}

		long startTimeInMillis = Calendar.getInstance().getTimeInMillis();
		int syslogsSent = sendSyslogs(startTimeInMillis);
		systemReport(startTimeInMillis, syslogsSent);
	}

	private static void systemReport(long beginMillis, int syslogsSent) {

		System.out.println("  Syslogs sent: " + syslogsSent);
		long totalMillis = Calendar.getInstance().getTimeInMillis() - beginMillis;
		Long totalSeconds = totalMillis / 1000L;
		System.out.println("Total Elapsed time (secs): " + totalSeconds);
		System.out.println();
	}

	private static int sendSyslogs(long beginMillis) throws IllegalStateException, InterruptedException, SQLException {

		m_sleepMillis = 0;
		int totalSyslogsSent = 0;

		topicName = "syslogd";
		Properties props = new Properties();
		props.put("bootstrap.servers", m_agentAddress.toString() + ":9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		producer = new KafkaProducer<String, String>(props);

		System.out.println("Sending " + m_syslogCount + " syslogs in " + m_batchCount
				+ " batches with a batch interval of " + m_batchDelay.toString() + " seconds...");
		for (int i = 1; i <= m_batchCount; i++) {

			Long batchBegin = Calendar.getInstance().getTimeInMillis();
			Double currentRate = 0.0;
			Integer batchSyslogsSent = 0;
			Long batchElapsedMillis = 0L;
			System.out.println("Sending batch " + i + " of " + Integer.valueOf(m_batchCount) + " batches of "
					+ m_batchSize.intValue() + " syslogs at the rate of " + m_syslogRate.toString() + " syslogs/sec...");
			System.out.println("m_batchSize : " + m_batchSize.doubleValue());
			System.out.println("m_syslogRate : " + m_syslogRate.doubleValue());

			System.out.println(
					"Estimated time to send: " + m_batchSize.doubleValue() / m_syslogRate.doubleValue() + " seconds");

			while (batchSyslogsSent.intValue() < m_batchSize.intValue()) {

				if (currentRate <= m_syslogRate || batchElapsedMillis == 0) {
					batchSyslogsSent += sendSyslog();
				} else {
					Thread.sleep(1);
					m_sleepMillis++;
				}

				batchElapsedMillis = Calendar.getInstance().getTimeInMillis() - batchBegin;
				currentRate = batchSyslogsSent.doubleValue() / batchElapsedMillis.doubleValue() * 1000.0;

				if (batchElapsedMillis % 1000 == 0) {
					System.out.print(".");
				}

			}

			System.out.println();
			totalSyslogsSent += batchSyslogsSent;
			System.out.println("   Actual time to send: " + (batchElapsedMillis / 1000.0 + " seconds"));
			System.out.println("Elapsed Time (secs): " + ((System.currentTimeMillis() - beginMillis) / 1000L));
			System.out.println("        Syslogs sent: " + Integer.valueOf(totalSyslogsSent).toString());
			System.out.println();
			Thread.sleep(m_batchDelay.longValue() * 1000L);
			m_sleepMillis += m_batchDelay.longValue() * 1000L;
		}

		int remainingSyslogs = m_syslogCount - totalSyslogsSent;
		//System.out.println("Sending batch remainder of " + remainingSyslogs + " syslogs...");
		Long batchBegin = Calendar.getInstance().getTimeInMillis();
		Double currentRate = 0.0;
		Long batchSyslogsSent = 0L;
		Long elapsedMillis = 0L;
		while (batchSyslogsSent.intValue() < remainingSyslogs) {

			if (currentRate <= m_syslogRate || elapsedMillis == 0) {
				batchSyslogsSent += sendSyslog();
			} else {
				Thread.sleep(1);
				m_sleepMillis++;
			}

			elapsedMillis = Calendar.getInstance().getTimeInMillis() - batchBegin;
			currentRate = batchSyslogsSent.doubleValue() / elapsedMillis.doubleValue() * 1000.0;
		}

		totalSyslogsSent += batchSyslogsSent;
		System.out.println("Elapsed Time (secs): " + ((System.currentTimeMillis() - beginMillis) / 1000L));
		System.out.println("         Syslogs sent: " + Integer.valueOf(totalSyslogsSent).toString());
		return totalSyslogsSent;
	}

	private static int sendSyslog() {
		int syslogsSent = 0;
		try {

			producer.send(new ProducerRecord<String, String>(topicName, syslogMessage));
			syslogsSent++;
		} catch (Exception e) {
			throw new IllegalStateException("Caught Exception sending syslog.", e);
		}
		return syslogsSent;
	}

	public abstract static class InetAddrUtils {
		public static String str(InetAddress address) {
			return address == null ? null : address.getHostAddress();
		}

		public static InetAddress addr(String value) {
			try {
				return value == null ? null : InetAddress.getByName(value);
			} catch (UnknownHostException e) {
				throw new RuntimeException("Unable to turn " + value + " into an inet address");
			}
		}

		public static InetAddress getLocalHostAddress() {
			return addr("127.0.0.1");
		}
	}

	public static String iso8601OffsetString(Date d, ZoneId zone, ChronoUnit truncateTo) {
		ZonedDateTime zdt = ((d).toInstant()).atZone(zone);
		if (truncateTo != null) {
			zdt = zdt.truncatedTo(truncateTo);
		}
		return zdt.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
	}

	public static String stripExtraQuotes(String string) {
		return string.replaceAll("^\"(.*)\"$", "$1");
	}

}
