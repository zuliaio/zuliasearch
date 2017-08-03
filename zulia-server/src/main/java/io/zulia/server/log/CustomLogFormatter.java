package io.zulia.server.log;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.MessageFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class CustomLogFormatter extends Formatter {

	private static final MessageFormat messageFormat = new MessageFormat("{3,date,yyyy-MM-dd hh:mm:ss.SSS} [{2}] <{1}> {5}: {4} {6}\n");

	public CustomLogFormatter() {
		super();
	}

	@Override
	public String format(LogRecord record) {
		Object[] arguments = new Object[7];
		arguments[0] = record.getLoggerName();
		arguments[1] = record.getLevel();
		arguments[2] = Thread.currentThread().getName();
		arguments[3] = new Date(record.getMillis());
		arguments[4] = record.getMessage();
		arguments[5] = record.getSourceClassName();
		if (record.getThrown() != null) {
			arguments[4] = "";
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			record.getThrown().printStackTrace(pw);
			arguments[6] = sw.toString();
		}
		else {
			arguments[4] = record.getMessage();
			arguments[6] = "";
		}
		return messageFormat.format(arguments);
	}

}