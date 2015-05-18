package net.iponweb.disthene.carbon;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.CharsetUtil;
import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.config.Rollup;
import net.iponweb.disthene.service.store.MetricStore;
import org.apache.log4j.Logger;

/**
 * @author Andrei Ivanov
 */
public class CarbonServerHandler extends ChannelInboundHandlerAdapter {
    Logger logger = Logger.getLogger(CarbonServerHandler.class);

    private MetricStore metricStore;
    private Rollup baseRollup;

    public CarbonServerHandler(MetricStore metricStore, Rollup baseRollup) {
        this.metricStore = metricStore;
        this.baseRollup = baseRollup;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf in = (ByteBuf) msg;
        Metric metric = new Metric(in.toString(CharsetUtil.UTF_8).trim(), baseRollup);
        metricStore.write(metric);
    }
}
