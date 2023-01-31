package io.zulia.client.pool;

import io.zulia.message.ZuliaBase.Node;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

public class ZuliaConnectionFactory extends BasePooledObjectFactory<ZuliaConnection> {

    private final boolean compressedConnection;
    private final Node node;

    public ZuliaConnectionFactory(Node node, boolean compressedConnection) {
        this.compressedConnection = compressedConnection;
        this.node = node;
    }

    @Override
    public ZuliaConnection create() {
        ZuliaConnection lc = new ZuliaConnection(node);
        lc.open(compressedConnection);
        return lc;
    }

    @Override
    public PooledObject<ZuliaConnection> wrap(ZuliaConnection obj) {
        {
            return new DefaultPooledObject<>(obj);
        }
    }

    @Override
    public void destroyObject(PooledObject<ZuliaConnection> p) {
        p.getObject().close();
    }
}
