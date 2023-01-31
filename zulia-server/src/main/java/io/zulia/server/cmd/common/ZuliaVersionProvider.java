package io.zulia.server.cmd.common;

import io.zulia.util.ZuliaVersion;
import picocli.CommandLine;

public class ZuliaVersionProvider implements CommandLine.IVersionProvider {
    @Override
    public String[] getVersion() {
        return new String[]{ZuliaVersion.getVersion()};
    }
}
