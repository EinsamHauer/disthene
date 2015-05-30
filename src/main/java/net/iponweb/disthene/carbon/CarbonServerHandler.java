package net.iponweb.disthene.carbon;

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

/**
 * @author Andrei Ivanov
 */
public class CarbonServerHandler extends ChannelInboundHandlerAdapter {
    private Logger logger = Logger.getLogger(CarbonServerHandler.class);

    private MBassador<DistheneEvent> bus;
    private Rollup rollup;

    public CarbonServerHandler(MBassador<DistheneEvent> bus, Rollup rollup) {
        this.bus = bus;
        this.rollup = rollup;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf in = (ByteBuf) msg;

        try {
            Metric metric = new Metric(in.toString(CharsetUtil.UTF_8).trim(), rollup);
            bus.post(new MetricReceivedEvent(metric)).asynchronously();
        } catch (Exception e) {
            logger.debug(e);
        }

        in.release();

    }
}
