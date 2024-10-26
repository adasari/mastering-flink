package org.example.window;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PeriodicWatermarkGenerator<T> implements WatermarkGenerator<T> {
    public static final Logger logger = LoggerFactory.getLogger(PeriodicWatermarkGenerator.class);
    private transient boolean isRunning = false;

    @Override
    public void onEvent(T t, long l, WatermarkOutput watermarkOutput) {
        if (!isRunning) {
            isRunning = true;
            logger.info("Emitting watermark onEvent {}", System.currentTimeMillis());
            watermarkOutput.emitWatermark(new Watermark(System.currentTimeMillis()));
        }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
        logger.info("Emitting watermark {}", System.currentTimeMillis());
        watermarkOutput.emitWatermark(new Watermark(System.currentTimeMillis()));
    }
}
