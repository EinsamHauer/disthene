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
import net.iponweb.disthene.service.auth.TenantService;
import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;

/**
 * @author Andrei Ivanov
 */
public class CarbonServerHandler extends ChannelInboundHandlerAdapter {
    private static final Logger logger = Logger.getLogger(CarbonServerHandler.class);

    @SuppressWarnings("UnstableApiUsage")
    private static final CharMatcher PRINTABLE_WITHOUT_SPACE = CharMatcher.inRange('\u0021', '\u007e');

    private MBassador<DistheneEvent> bus;
    private Rollup rollup;
    private TenantService tenantService;

    public CarbonServerHandler(MBassador<DistheneEvent> bus, Rollup rollup, TenantService tenantService) {
        this.bus = bus;
        this.rollup = rollup;
        this.tenantService = tenantService;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf in = (ByteBuf) msg;

        try {
            final Metric metric = new Metric(in.toString(CharsetUtil.UTF_8).trim(), rollup);
            if ((System.currentTimeMillis() / 1000L) - metric.getTimestamp() > 3600) {
                logger.warn("Metric is from distant past (older than 1 hour): " + metric);
            }

            boolean isValid = true;

            if (!tenantService.isTenantAllowed(metric.getTenant())) {
                isValid = false;
                logger.warn("Unauthorized tenant: " + metric.getTenant() + ". Discarding metric: " + metric);
            }

            if (!PRINTABLE_WITHOUT_SPACE.matchesAllOf(metric.getPath())) {
                isValid = false;
                logger.warn("Non printable characters in metric, discarding: " + metric + " (" + Hex.encodeHexString(metric.getPath().getBytes()) + ")");
            }

            if (isValid) {
                bus.post(new MetricReceivedEvent(metric)).now();
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
            logger.trace("Exception caught in carbon handler", cause);
        } else {
            logger.error("Exception caught in carbon handler", cause);
        }
    }
}
