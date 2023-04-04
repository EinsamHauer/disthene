package net.iponweb.disthene.carbon;

import com.google.common.base.CharMatcher;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.CharsetUtil;
import net.engio.mbassy.bus.MBassador;
import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.config.Rollup;
import net.iponweb.disthene.events.DistheneEvent;
import net.iponweb.disthene.events.MetricReceivedEvent;
import org.apache.commons.codec.binary.Hex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;

/**
 * @author Andrei Ivanov
 */
public class CarbonServerHandler extends ChannelInboundHandlerAdapter {
    
    private static final Logger logger = LogManager.getLogger(CarbonServerHandler.class);

    private static final CharMatcher PRINTABLE_WITHOUT_SPACE = CharMatcher.inRange('\u0021', '\u007e');

    private final MBassador<DistheneEvent> bus;
    private final Rollup rollup;

    public CarbonServerHandler(MBassador<DistheneEvent> bus, Rollup rollup) {
        this.bus = bus;
        this.rollup = rollup;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf in = (ByteBuf) msg;

        try {
            final Metric metric = new Metric(in.toString(CharsetUtil.UTF_8).trim(), rollup);
            long metricAge = (System.currentTimeMillis() / 1000L) - metric.getTimestamp();

            boolean isValid = true;

            if (metricAge > 3600) {
                if (metricAge > 7200) {
                    isValid = false;
                    logger.warn("Metric is from distant past (older than 2 hours). Discarding metric: " + metric);
                } else {
                    logger.warn("Metric is from distant past (older than 1 hour): " + metric);
                }
            }

            if (!PRINTABLE_WITHOUT_SPACE.matchesAllOf(metric.getPath())) {
                isValid = false;
                logger.warn("Non printable characters in metric, discarding: " + metric + " (" + Hex.encodeHexString(metric.getPath().getBytes()) + ")");
            }

            if (isValid) {
                bus.post(new MetricReceivedEvent(metric)).now();
            } else {
                logger.warn("Non ASCII characters received, discarding: " + metric);
            }
        } catch (Exception e) {
            logger.trace(e);
        }

        in.release();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.channel().close();

        if (cause instanceof java.io.IOException) {
            logger.trace("Exception caught in carbon handler (connection from " + ((InetSocketAddress)ctx.channel().remoteAddress()).getAddress().getHostAddress() + ")", cause);
        } else {
            logger.error("Exception caught in carbon handler", cause);
        }
    }
}
