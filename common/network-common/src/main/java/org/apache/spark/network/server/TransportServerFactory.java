/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.server;

import java.io.Closeable;
import java.util.List;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.EventLoopGroup;

import org.apache.spark.network.TransportContext;
import org.apache.spark.network.util.IOMode;
import org.apache.spark.network.util.NettyUtils;

/**
 * Servers with independent ports and handlers sharing I/O workers and an allocator.
 * Closing one server closes only its listener and accepted channels. Closing this factory shuts
 * down every server's I/O workers; the caller retains ownership of the transport context.
 */
public final class TransportServerFactory implements Closeable {
  private final TransportContext context;
  final EventLoopGroup bossGroup;
  final EventLoopGroup workerGroup;
  final PooledByteBufAllocator allocator;
  private boolean closed;

  public TransportServerFactory(TransportContext context) {
    this.context = context;
    var conf = context.getConf();
    IOMode ioMode = IOMode.valueOf(conf.ioMode());
    allocator = TransportServer.createAllocator(conf);
    bossGroup = NettyUtils.createEventLoop(ioMode, 1, conf.getModuleName() + "-boss");
    workerGroup = NettyUtils.createEventLoop(
      ioMode, conf.serverThreads(), conf.getModuleName() + "-server");
  }

  /** Bind an independent server to an ephemeral port. Creation cannot race factory shutdown. */
  public synchronized TransportServer createServer(RpcHandler handler) {
    if (closed) {
      throw new IllegalStateException("Transport server factory is closed");
    }
    return new TransportServer(context, null, 0, handler, List.of(), this);
  }

  @Override
  public synchronized void close() {
    if (!closed) {
      closed = true;
      bossGroup.shutdownGracefully();
      workerGroup.shutdownGracefully();
    }
  }
}
