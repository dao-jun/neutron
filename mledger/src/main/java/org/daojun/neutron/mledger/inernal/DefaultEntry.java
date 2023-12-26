package org.daojun.neutron.mledger.inernal;

import io.netty.buffer.ByteBuf;
import org.daojun.neutron.mledger.Entry;
import org.daojun.neutron.storage.Position;

public class DefaultEntry implements Entry {
    private final ByteBuf data;
    private final Position position;

    private DefaultEntry(ByteBuf data, Position position) {
        this.data = data;
        this.position = position;
    }

    public static DefaultEntry create(ByteBuf data, Position position) {
        return new DefaultEntry(data, position);
    }

    @Override
    public int getLength() {
        return this.data.readableBytes();
    }

    @Override
    public Position getPosition() {
        return this.position;
    }

    @Override
    public ByteBuf getData() {
        return this.data;
    }

    @Override
    public boolean release() {
        return this.data.release();
    }
}
