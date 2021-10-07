package io.zulia.server.test.mongo;

import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfig;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import io.zulia.server.test.node.TestHelper;

public class MongoTestInstance {

    private static final String LOCAL_INSTANCE_URL_HOST = "mongodb://127.0.0.1";
    private static MongodStarter mongodStarter = MongodStarter.getDefaultInstance();

    private Integer port;
    private String testInstanceUrl;
    private MongodProcess mongodProcess;

    public MongoTestInstance() { }

    public String getInstanceUrl() {
        return testInstanceUrl;
    }

    public void shutdown() {
        if(mongodProcess != null) {
            mongodProcess.stop();
        }
    }

    public void start() {

        try {

            port = Network.getFreeServerPort();

            MongodConfig mongodConfig = MongodConfig.builder()
                    .version(Version.Main.PRODUCTION)
                    .net(new Net(port, Network.localhostIsIPv6()))
                    .build();


            MongodExecutable mongodExecutable = mongodStarter.prepare(mongodConfig);
            mongodProcess = mongodExecutable.start();

            testInstanceUrl = buildTestInstanceUrl();

        } catch (Exception ex) {
            throw new IllegalStateException("Unable to start the test MongoDB instance", ex);
        }

    }

    private String buildTestInstanceUrl() {
        return LOCAL_INSTANCE_URL_HOST + ":" + port;
    }

}

