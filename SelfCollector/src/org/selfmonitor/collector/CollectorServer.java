/*
 * This file is part of SelfMonitor
 *
 * Copyright (c) 2018 Andrew Di <anonymous-oss@outlook.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package org.selfmonitor.collector;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import org.selfmonitor.collector.handler.CollectorInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CollectorServer {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    //Netty Listen Port
    private int port;

    //Number of Worker Threads
    private int workCount = 128;

    private ServerBootstrap serverBootstrap;
    private ChannelFuture channelFuture;

    public void setWorkCount(int workCount) {
        this.workCount = workCount;
    }

    public int getWorkCount() {
        return workCount;
    }

    public CollectorServer(){
        this.port=11111;
    }

    public CollectorServer(int port){
        this.port=port;
    }

    //Server Run Entry
    public void run() throws Exception{
        EventLoopGroup masterGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(masterGroup,workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel socketChannel) {
                            /**
                             * http-request解码器
                             * http服务器端对request解码
                             */
                            socketChannel.pipeline().addLast("decoder", new HttpRequestDecoder());
                            /**
                             * http-response解码器
                             * http服务器端对response编码
                             */
                            socketChannel.pipeline().addLast("encoder", new HttpResponseEncoder());
                            socketChannel.pipeline().addLast("aggregator", new HttpObjectAggregator(1048576));
                            /**
                             * 压缩
                             * Compresses an HttpMessage and an HttpContent in gzip or deflate encoding
                             * while respecting the "Accept-Encoding" header.
                             * If there is no matching encoding, no compression is done.
                             */
                            socketChannel.pipeline().addLast("deflater", new HttpContentCompressor());

                            socketChannel.pipeline().addLast("handler", new CollectorInboundHandler());
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG,this.workCount)
                    .childOption(ChannelOption.SO_KEEPALIVE,true);

            log.info("Collector Server is starting.");
            channelFuture=serverBootstrap.bind(port).sync();
            channelFuture.channel().closeFuture().sync();
        }
        finally {
            workerGroup.shutdownGracefully();
            masterGroup.shutdownGracefully();
            log.info("Collector Server has stopped.");
        }
    }

    public void shutdown() throws Exception{

    }
}
