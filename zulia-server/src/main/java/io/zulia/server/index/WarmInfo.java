package io.zulia.server.index;

record WarmInfo(boolean needsWarming, Long lastChanged, Long lastCommit) {

}