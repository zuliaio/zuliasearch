package io.zulia.server.util.log;

import io.zulia.server.ZuliaD;

import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Payam Meyer on 8/3/17.
 * @author pmeyer
 */
public class LogUtil {

	public static void init() {
		Logger logger = Logger.getLogger(ZuliaD.class.getName());
		logger.setUseParentHandlers(false);

		CustomLogFormatter formatter = new CustomLogFormatter();
		ConsoleHandler handler = new ConsoleHandler();
		handler.setFormatter(formatter);

		logger.addHandler(handler);

		Logger.getLogger("org.mongodb").setLevel(Level.WARNING);
	}

}
