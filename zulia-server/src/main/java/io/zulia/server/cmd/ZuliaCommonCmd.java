package io.zulia.server.cmd;

import io.zulia.log.LogUtil;
import io.zulia.server.cmd.common.SelectiveStackTraceHandler;
import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi.Style;
import picocli.CommandLine.Help.ColorScheme;

public class ZuliaCommonCmd {
	private final static ColorScheme COLOR_SCHEME = new ColorScheme.Builder()
			.commands    (Style.bold, Style.fg_blue)    // combine multiple styles
			.options     (Style.fg_magenta)                // yellow foreground color
			.parameters  (Style.fg_magenta)
			.optionParams(Style.italic)
			.errors      (Style.fg_red, Style.bold)
			.stackTraces (Style.italic)
			.applySystemProperties()
			.build();

	public static void printMagenta(String text) {
		System.out.println(CommandLine.Help.Ansi.AUTO.string("@|bold,fg(fg_magenta) " + text + "|@"));
	}

	public static void printBlue(String text) {
		System.out.println(CommandLine.Help.Ansi.AUTO.string("@|bold,fg(blue) " + text + "|@"));
	}

	public static void printOrange(String text) {
		System.out.println(CommandLine.Help.Ansi.AUTO.string("@|bold,blue " + text + "|@"));
	}


	public static void runCommandLine(Object object, String[] args) {
		LogUtil.init();

		int exitCode = new CommandLine(object).setAbbreviatedSubcommandsAllowed(true).setAbbreviatedOptionsAllowed(true)
				.setExecutionExceptionHandler(new SelectiveStackTraceHandler()).setTrimQuotes(true).setColorScheme(COLOR_SCHEME).execute(args);

		if (exitCode != 0) { // don't close daemon on success, java will close in other cases
			System.exit(exitCode);
		}
	}

}
