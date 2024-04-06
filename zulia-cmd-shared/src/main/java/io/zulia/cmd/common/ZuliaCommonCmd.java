package io.zulia.cmd.common;

import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi.Style;
import picocli.CommandLine.Help.ColorScheme;

public class ZuliaCommonCmd {
	private final static ColorScheme COLOR_SCHEME = new ColorScheme.Builder().commands(Style.bold, Style.fg_blue)    // combine multiple styles
			.options(Style.fg_magenta)                // yellow foreground color
			.parameters(Style.fg_magenta).optionParams(Style.italic).errors(Style.fg_red, Style.bold).stackTraces(Style.italic).applySystemProperties().build();
	public static final String MAGENTA = "magenta";
	public static final String BLUE = "blue";
	public static final String ORANGE = "166";

	public static String getInColor(String color, String text) {
		return CommandLine.Help.Ansi.AUTO.string("@|bold,fg(" + color + ") " + text + "|@");
	}

	public static void printMagenta(String text) {
		System.out.println(getInColor(MAGENTA, text));
	}

	public static void printBlue(String text) {
		System.out.println(getInColor(BLUE, text));
	}

	public static void printOrange(String text) {
		System.out.println(getInColor(ORANGE, text));
	}

	public static void runCommandLine(Object object, String[] args) {

		int exitCode = new CommandLine(object).setAbbreviatedSubcommandsAllowed(true).setAbbreviatedOptionsAllowed(true)
				.setExecutionExceptionHandler(new SelectiveStackTraceHandler()).setTrimQuotes(true).setColorScheme(COLOR_SCHEME).execute(args);

		if (exitCode != 0) { // don't close daemon on success, java will close in other cases
			System.exit(exitCode);
		}
	}

}
