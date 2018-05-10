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
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.json.JsonObjectDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.proxy.HttpProxyHandler;
import org.selfmonitor.collector.handler.CollectorInboundHandler;


public class CollectorServer {
    //Netty Listen Port
    private int port;

    //Number of Worker Threads
    private int workCount = 8;

    private ServerBootstrap serverBootstrap;
    private ChannelFuture channelFuture;
    private CollectorInboundHandler collectorInboundHandler;

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
            collectorInboundHandler=new CollectorInboundHandler();
            serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(masterGroup,workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel socketChannel) {
                            socketChannel.pipeline().addLast(new JsonObjectDecoder());
                            //socketChannel.pipeline().addLast(new LineBasedFrameDecoder(1024*1024));
                            //socketChannel.pipeline().addLast(new StringDecoder());
                            socketChannel.pipeline().addLast(collectorInboundHandler);
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG,this.workCount)
                    .childOption(ChannelOption.SO_KEEPALIVE,true);

            channelFuture=serverBootstrap.bind(port).sync();
            channelFuture.channel().closeFuture().sync();
        }
        finally {
            workerGroup.shutdownGracefully();
            masterGroup.shutdownGracefully();
        }
    }

    public void shutdown() throws Exception{

    }
}
