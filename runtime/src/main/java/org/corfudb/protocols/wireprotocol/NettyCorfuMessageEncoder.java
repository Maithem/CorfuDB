package org.corfudb.protocols.wireprotocol;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AtomicDouble;
import com.google.protobuf.TextFormat;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAccumulator;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;

import static org.corfudb.protocols.CorfuProtocolCommon.MessageMarker.PROTO_REQUEST_MSG_MARK;
import static org.corfudb.protocols.CorfuProtocolCommon.MessageMarker.PROTO_RESPONSE_MSG_MARK;

/**
 * Created by mwei on 10/1/15.
 */
@Slf4j
public class NettyCorfuMessageEncoder extends MessageToByteEncoder<Object> {

    private static final Optional<Timer> encoder = MeterRegistryProvider.getInstance().map(registry ->
                        io.micrometer.core.instrument.Timer.builder("NettyCorfuMessageEncoder.latency")
                                .publishPercentiles(0.50, 0.95, 0.99)
                                .publishPercentileHistogram(true)
                                .register(registry));

    private static final Optional<DistributionSummary> writeDistributionSummary = MeterRegistryProvider
            .getInstance()
            .map(registry -> DistributionSummary.builder("encodesizes")
                    .baseUnit("bytes").register(registry));


    /**
     * Encodes an outbound corfu message into a ByteBuf. The corfu message is either
     * legacy (of type CorfuMsg) or Protobuf (of type RequestMsg/ResponseMsg).
     *
     * @param channelHandlerContext   the Netty channel handler context
     * @param object                  the object being encoded
     * @param byteBuf                 the underlying ByteBuf
     */
    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext, Object object, ByteBuf byteBuf) {
        long ts = System.nanoTime();
        try {
            if (object instanceof RequestMsg) {
                RequestMsg request = (RequestMsg) object;
                long start = byteBuf.writerIndex();
                try (ByteBufOutputStream requestOutputStream = new ByteBufOutputStream(byteBuf)) {
                    try {
                        // Marks the Corfu msg as a protobuf request.
                        requestOutputStream.writeByte(PROTO_REQUEST_MSG_MARK.asByte());
                        requestOutputStream.writeLong(System.nanoTime());
                        request.writeTo(requestOutputStream);
                        writeDistributionSummary.ifPresent(x -> x.record(byteBuf.writerIndex() - start));
                    } catch (IOException e) {
                        log.warn("encode[{}]: Exception occurred when encoding request {}, caused by {}",
                                request.getHeader().getRequestId(), TextFormat.shortDebugString(request.getHeader()),
                                e.getCause(), e);
                    }
                }
            } else if (object instanceof ResponseMsg) {
                ResponseMsg response = (ResponseMsg) object;

                try (ByteBufOutputStream responseOutputStream = new ByteBufOutputStream(byteBuf)) {
                    try {
                        // Marks the Corfu msg as protobuf response.
                        responseOutputStream.writeByte(PROTO_RESPONSE_MSG_MARK.asByte());
                        responseOutputStream.writeLong(System.nanoTime());
                        response.writeTo(responseOutputStream);
                    } catch (IOException e) {
                        log.warn("encode[{}]: Exception occurred when encoding response {}, caused by {}",
                                response.getHeader().getRequestId(), TextFormat.shortDebugString(response.getHeader()),
                                e.getCause(), e);
                    }
                }
            } else {
                log.error("encode: Unknown object of class - {} received while encoding", object.getClass());
            }

            encoder.ifPresent(x -> x.record(System.nanoTime() - ts, TimeUnit.NANOSECONDS));

        } catch (Exception e) {
            log.error("encode: Error during serialization!", e);
        }
    }
}
