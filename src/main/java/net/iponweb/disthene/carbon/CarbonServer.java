package net.iponweb.disthene.carbon;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import net.iponweb.disthene.config.DistheneConfiguration;
import net.iponweb.disthene.service.store.MetricStore;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author Andrei Ivanov
 */

@Component
public class CarbonServer {
    public static final int MAX_FRAME_LENGTH = 1024;
    Logger logger = Logger.getLogger(CarbonServer.class);

    @Autowired
    private DistheneConfiguration configuration;

    @Autowired
    MetricStore metricStore;

    EventLoopGroup bossGroup = new NioEventLoopGroup(100);
    EventLoopGroup workerGroup = new NioEventLoopGroup();
    private ChannelFuture channelFuture;

    public void run() throws InterruptedException {
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 100)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new DelimiterBasedFrameDecoder(MAX_FRAME_LENGTH, false, Delimiters.lineDelimiter()));
                        p.addLast(new CarbonServerHandler(metricStore, configuration.getCarbon().getBaseRollup()));
                    }
                });

        // Start the server.
        channelFuture = b.bind(configuration.getCarbon().getPort()).sync();
    }

    public void shutdown() {
        ChannelFuture f = channelFuture.channel().close();
        f.awaitUninterruptibly();
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }
}
