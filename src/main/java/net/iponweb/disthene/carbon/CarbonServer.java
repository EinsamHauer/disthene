package net.iponweb.disthene.carbon;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.util.concurrent.Future;
import net.engio.mbassy.bus.MBassador;
import net.iponweb.disthene.config.DistheneConfiguration;
import net.iponweb.disthene.events.DistheneEvent;
import org.apache.log4j.Logger;

/**
 * @author Andrei Ivanov
 */

public class CarbonServer {
    public static final int MAX_FRAME_LENGTH = 102400;
    private Logger logger = Logger.getLogger(CarbonServer.class);

    private DistheneConfiguration configuration;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ChannelFuture channelFuture;
    private MBassador<DistheneEvent> bus;

    public CarbonServer(DistheneConfiguration configuration, MBassador<DistheneEvent> bus) {
        this.bus = bus;
        this.configuration = configuration;

        bossGroup = new NioEventLoopGroup(configuration.getCarbon().getThreads());
        workerGroup = new NioEventLoopGroup(configuration.getCarbon().getThreads() * 5);
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
                        p.addLast(new CarbonServerHandler(bus, configuration.getCarbon().getBaseRollup()));
                    }

                    @Override
                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                        logger.error(cause);
                        super.exceptionCaught(ctx, cause);
                    }
                });

        // Start the server.
        channelFuture = b.bind(configuration.getCarbon().getPort()).sync();
    }

    public void shutdown() {
        try {
            ChannelFuture f = channelFuture.channel().close();
            logger.info("Closing channel");
            f.awaitUninterruptibly(60000);
        } catch (Exception e) {
            logger.error("We failed to close channel. It may still be OK though");
            logger.error(e);
        }

        Future bossGroupShutdownFuture = bossGroup.shutdownGracefully();
        logger.info("Shutting down boss group");
        bossGroupShutdownFuture.awaitUninterruptibly(60000);

        Future workerGroupShutdownFuture = workerGroup.shutdownGracefully();
        logger.info("Shutting down worker group");
        workerGroupShutdownFuture.awaitUninterruptibly(180000);
    }
}
