package io.zulia.server.log;

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Payam Meyer on 8/3/17.
 * @author pmeyer
 */
public class LogUtil {

	public static void init() {
		Logger logger = Logger.getLogger("");

		for (Handler h : logger.getHandlers()) {
			CustomLogFormatter formatter = new CustomLogFormatter();
			h.setFormatter(formatter);
		}

		Logger.getLogger("org.mongodb").setLevel(Level.WARNING);

		System.setProperty("hazelcast.logging.type", "none");
	}

}
