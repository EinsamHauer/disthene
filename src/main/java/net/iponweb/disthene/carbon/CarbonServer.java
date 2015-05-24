package net.iponweb.disthene.carbon;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import net.iponweb.disthene.config.DistheneConfiguration;
import net.iponweb.disthene.service.aggregate.Aggregator;
import net.iponweb.disthene.service.blacklist.BlackList;
import net.iponweb.disthene.service.general.GeneralStore;
import net.iponweb.disthene.service.index.IndexStore;
import net.iponweb.disthene.service.store.MetricStore;
import org.apache.log4j.Logger;

/**
 * @author Andrei Ivanov
 */

public class CarbonServer {
    public static final int MAX_FRAME_LENGTH = 1024;
    private Logger logger = Logger.getLogger(CarbonServer.class);

    private DistheneConfiguration configuration;
    private GeneralStore generalStore;

    private EventLoopGroup bossGroup = new NioEventLoopGroup(500);
    private EventLoopGroup workerGroup = new NioEventLoopGroup(500);
    private ChannelFuture channelFuture;

    public CarbonServer(DistheneConfiguration configuration, GeneralStore generalStore) {
        this.configuration = configuration;
        this.generalStore = generalStore;
    }

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
                        p.addLast(new CarbonServerHandler(generalStore, configuration.getCarbon().getBaseRollup()));
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
