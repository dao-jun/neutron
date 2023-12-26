package org.daojun.neutron.mledger;

import io.netty.buffer.ByteBuf;
import org.daojun.neutron.storage.Position;

public interface Entry {

    int getLength();

    Position getPosition();

    ByteBuf getData();

    boolean release();
}
