package io.zulia.client.pool;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaServiceGrpc;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ZuliaConnection {

    private static final Logger LOG = Logger.getLogger(ZuliaConnection.class.getName());

    private Node node;

    private ManagedChannel channel;

    private ZuliaServiceGrpc.ZuliaServiceBlockingStub blockingStub;
    private ZuliaServiceGrpc.ZuliaServiceStub asyncStub;

    private final long connectionNumber;

    private static AtomicLong connectionNumberGen = new AtomicLong();

    public ZuliaConnection(Node node) {
        this.node = node;
        this.connectionNumber = connectionNumberGen.getAndIncrement();
    }

    public void open(boolean compressedConnection) {

        ManagedChannelBuilder<?> managedChannelBuilder = ManagedChannelBuilder.forAddress(node.getServerAddress(), node.getServicePort())
                .maxInboundMessageSize(256 * 1024 * 1024).usePlaintext();
        channel = managedChannelBuilder.build();

        blockingStub = ZuliaServiceGrpc.newBlockingStub(channel);
        if (compressedConnection) {
            blockingStub = blockingStub.withCompression("gzip");
        }

        asyncStub = ZuliaServiceGrpc.newStub(channel);
        if (compressedConnection) {
            asyncStub = asyncStub.withCompression("gzip");
        }

        LOG.info("Connecting to <" + node.getServerAddress() + ":" + node.getServicePort() + "> id: " + connectionNumber);

    }

    public ZuliaServiceGrpc.ZuliaServiceBlockingStub getService() {
        return blockingStub;
    }

    public ZuliaServiceGrpc.ZuliaServiceStub getAsyncService() {
        return asyncStub;
    }

    /**
     * closes the connection to the server if open, calling a method (index, query, ...) will open a new connection
     */
    public void close() {

        LOG.info("Closing connection to <" + node.getServerAddress() + ":" + node.getServicePort() + "> id: " + connectionNumber);
        try {
            if (channel != null) {
                channel.shutdownNow();
            }
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Exception closing: ", e);
        }
        channel = null;
        blockingStub = null;
        asyncStub = null;

    }

}
