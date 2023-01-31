package io.zulia.server.cmd.common;

import picocli.CommandLine;

public class SelectiveStackTraceHandler implements CommandLine.IExecutionExceptionHandler {

    @Override
    public int handleExecutionException(Exception ex, CommandLine commandLine, CommandLine.ParseResult parseResult) {

        CommandLine.Help.ColorScheme colorScheme = commandLine.getColorScheme();

        System.out.println(colorScheme.errorText(""));
        System.out.println(colorScheme.errorText(ex.getMessage()));
        CommandLine.Model.OptionSpec showStack = commandLine.getCommandSpec().findOption("showStack");
        if (showStack.getValue()) {
            ex.printStackTrace();
        }

        return commandLine.getCommandSpec().exitCodeOnExecutionException();
    }
}