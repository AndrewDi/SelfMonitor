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

package org.selfmonitor.collector.handler;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import io.netty.handler.codec.http.multipart.HttpDataFactory;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import io.netty.util.CharsetUtil;
import org.selfmonitor.collector.MessageType;
import org.selfmonitor.collector.utils.MqUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;

import java.util.Collections;
import java.util.Set;

import static io.netty.buffer.Unpooled.copiedBuffer;
import static io.netty.handler.codec.http.HttpHeaderNames.*;

@ChannelHandler.Sharable
public class CollectorInboundHandler extends ChannelInboundHandlerAdapter {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private HttpPostRequestDecoder decoder;
    private FullHttpRequest fullHttpRequest;
    private final StringBuilder responseContent = new StringBuilder();
    private static final HttpDataFactory factory = new DefaultHttpDataFactory(DefaultHttpDataFactory.MINSIZE);


    public CollectorInboundHandler() {
        super();
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (decoder != null) {
            decoder.cleanFiles();
        }
    }

    /***
     * Receive message and put it into MQ
     * @param ctx
     * @param msg
     * @throws Exception
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if(msg instanceof FullHttpRequest){
            fullHttpRequest = (FullHttpRequest)msg;
            ByteBuf msgBuf = fullHttpRequest.content();
            String msgStr = msgBuf.toString(io.netty.util.CharsetUtil.UTF_8);
            try {
                MqUtils mqUtils = new MqUtils();
                if(msgStr.startsWith("[")) {
                    JSONArray msgJson = JSON.parseArray(msgStr);
                    for (int i = 0; i < msgJson.size(); i++) {
                        JSONObject data = msgJson.getJSONObject(i);
                        MessageType type = MessageType.valueOf(data.getString("type"));
                        String topic = data.getString("topic");
                        if (topic != null && type != null) {
                            if (!mqUtils.mqPut(msgStr, topic, type.getName())) {
                                responseContent.append("MQ Put Error:" + data.toJSONString());
                            }
                        }
                    }
                }
                else {
                    JSONObject data = JSON.parseObject(msgStr);
                    MessageType type = MessageType.valueOf(data.getString("type"));
                    String topic = data.getString("topic");
                    if (topic != null && type != null) {
                        if (!mqUtils.mqPut(msgStr, topic, type.getName())) {
                            responseContent.append("MQ Put Error:" + data.toJSONString());
                        }
                    }
                }
                responseContent.append("{\"return_code\":200}");
                writeResponse(ctx.channel());
            }
            catch (JSONException jsonException){
                //Write invalid Msg Error to HTTPResponse
                log.error("Invalid Json String Received:"+jsonException.getLocalizedMessage()+" Origin Message:"+msgStr);
                responseContent.append("Invalid Json String Received\n");
                writeResponse(ctx.channel());
            }
            finally {
                msgBuf.release();
            }

        }
        else {
            log.error("Unknow Request:"+msg.toString());
            ctx.close();
        }

    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        super.channelReadComplete(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error(cause.getLocalizedMessage());
        ctx.close();
    }

    private void writeResponse(Channel channel){


        // Convert the response content to a ChannelBuffer.
        ByteBuf buf = copiedBuffer(responseContent.toString(), CharsetUtil.UTF_8);

        responseContent.setLength(0);

        boolean close = fullHttpRequest.headers().contains(CONNECTION, HttpHeaders.Values.CLOSE, true)
                || fullHttpRequest.getProtocolVersion().equals(HttpVersion.HTTP_1_0)
                && !fullHttpRequest.headers().contains(CONNECTION, HttpHeaders.Values.KEEP_ALIVE, true);


        // Build the response object.
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, buf);
        response.headers().set(CONTENT_TYPE, "text/json; charset=UTF-8");

        if (!close) {
            // There's no need to add 'Content-Length' header
            // if this is the last response.
            response.headers().set(CONTENT_LENGTH, buf.readableBytes());
        }

        Set<Cookie> cookies;

        String value = fullHttpRequest.headers().get(COOKIE);
        if (value == null) {
            cookies = Collections.emptySet();
        } else {
            cookies = CookieDecoder.decode(value);
        }
        if (!cookies.isEmpty()) {
            // Reset the cookies if necessary.
            for (Cookie cookie : cookies) {
                response.headers().add(SET_COOKIE, ServerCookieEncoder.encode(cookie));
            }
        }
        // Write the response.
        ChannelFuture future = channel.writeAndFlush(response);
        // Close the connection after the write operation is done if necessary.
        if (close) future.addListener(ChannelFutureListener.CLOSE);

    }
}
