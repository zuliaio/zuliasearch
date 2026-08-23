package io.zulia.cmd.common;

import ch.qos.logback.core.PropertyDefinerBase;

import java.io.Console;

/**
 * Supplies the console log pattern to logback.xml at configuration time.
 * <p>
 * The colored pattern is used only when the JVM is attached to an interactive terminal, so output that is piped,
 * redirected, or captured by a service manager stays free of ANSI escape codes. The check is pure Java
 * (System.console().isTerminal()) and needs no native library, so no native access warning is raised on Java 24+.
 * <p>
 * Set the system property {@code zulia.log.color} to {@code true} or {@code false} to force either mode, for example
 * to keep color when running under a container runtime that allocates a TTY.
 */
public class ConsoleLogPatternDefiner extends PropertyDefinerBase {

	public static final String COLOR_PROPERTY = "zulia.log.color";

	private static final String COLOR_PATTERN = "%cyan(%date{ISO8601}) %gray([%thread]) %highlight(%-5level) %magenta(%logger{36}) - %msg%n";
	private static final String PLAIN_PATTERN = "%date{ISO8601} [%thread] %-5level %logger{36} - %msg%n";

	@Override
	public String getPropertyValue() {
		return useColor() ? COLOR_PATTERN : PLAIN_PATTERN;
	}

	static boolean useColor() {
		String forced = System.getProperty(COLOR_PROPERTY);
		if (forced != null) {
			return Boolean.parseBoolean(forced);
		}
		return isInteractiveTerminal() && supportsAnsi();
	}

	private static boolean isInteractiveTerminal() {
		Console console = System.console();
		return console != null && console.isTerminal();
	}

	/**
	 * Every Unix terminal renders ANSI. A Windows console only does so under Windows Terminal or when a
	 * terminal emulator advertises itself through TERM, ConEmu, or ANSICON.
	 */
	private static boolean supportsAnsi() {
		if (!System.getProperty("os.name", "").startsWith("Windows")) {
			return true;
		}
		return System.getenv("WT_SESSION") != null || System.getenv("TERM") != null || System.getenv("ConEmuANSI") != null
				|| System.getenv("ANSICON") != null;
	}
}
