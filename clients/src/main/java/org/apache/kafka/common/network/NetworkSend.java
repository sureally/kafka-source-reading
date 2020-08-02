/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.network;

import java.nio.ByteBuffer;

/**
 * A size delimited Send that consists of a 4 byte network-ordered size N followed by N bytes of content
 * 继承了ByteBufferSend,增加了4字节表示内容大小(不包含这4byte)
 */
public class NetworkSend extends ByteBufferSend {

    public NetworkSend(String destination, ByteBuffer buffer) {
        super(destination, sizeDelimit(buffer));
    }

    private static ByteBuffer[] sizeDelimit(ByteBuffer buffer) {
        return new ByteBuffer[] {sizeBuffer(buffer.remaining()), buffer};
    }

    // 可理解kafka字节流的为：buffer=buffer1+buffer2
    // 其中buffer2是我们发送到broker端消息，buffer1是一个4字节的大小的数字，其含义是消息buffer2的大小
    // 最终在broker读取字节流时候，先去取出头四个字节，感知接下来需要读取多少字节。然后读取。
    private static ByteBuffer sizeBuffer(int size) {
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        sizeBuffer.putInt(size);
        sizeBuffer.rewind();
        return sizeBuffer;
    }

}
