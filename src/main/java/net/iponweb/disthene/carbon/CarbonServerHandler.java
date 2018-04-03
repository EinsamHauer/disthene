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
import org.apache.log4j.Logger;

import java.util.Set;

/**
 * @author Andrei Ivanov
 */
public class CarbonServerHandler extends ChannelInboundHandlerAdapter {
    private Logger logger = Logger.getLogger(CarbonServerHandler.class);

    private MBassador<DistheneEvent> bus;
    private Rollup rollup;
    private Set<String> authorizedTenants;
    private boolean allowAll;

    public CarbonServerHandler(MBassador<DistheneEvent> bus, Rollup rollup, Set<String> authorizedTenants, boolean allowAll) {
        this.bus = bus;
        this.rollup = rollup;
        this.authorizedTenants = authorizedTenants;
        this.allowAll = allowAll;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf in = (ByteBuf) msg;

        try {
            final Metric metric = new Metric(in.toString(CharsetUtil.UTF_8).trim(), rollup);
            if ((System.currentTimeMillis() / 1000L) - metric.getTimestamp() > 3600) {
                logger.warn("Metric is from distant past (older than 1 hour): " + metric);
            }

            boolean isValid = true;

            if (!allowAll && !authorizedTenants.contains(metric.getTenant())) {
                isValid = false;
                logger.error("Unauthorized tenant: " + metric.getTenant() + ". Discarding metric: " + metric);
            }

            if (!CharMatcher.ASCII.matchesAllOf(metric.getPath()) || !CharMatcher.ASCII.matchesAllOf(metric.getTenant())) {
                isValid = false;
                logger.warn("Non ASCII characters received, discarding: " + metric);
            }

            if (isValid) {
                bus.post(new MetricReceivedEvent(metric)).now();
            }
        } catch (Exception e) {
            logger.trace(e);
        }

        in.release();
    }
}
