package org.example.window;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomProcessingTimeTrigger extends Trigger<Object, TimeWindow> {
    private static final long serialVersionUID = 1L;

    public static final Logger logger = LoggerFactory.getLogger(CustomProcessingTimeTrigger.class);

    public CustomProcessingTimeTrigger() {
    }

    public TriggerResult onElement (Object element,long timestamp, TimeWindow window, Trigger.TriggerContext ctx){
        logger.info("On Processing onElement: {}, start: {}, end: {}", window.maxTimestamp(), window.getStart(), window.getEnd());
        ctx.registerProcessingTimeTimer(window.maxTimestamp());
        return TriggerResult.FIRE;
    }

    public TriggerResult onEventTime ( long time, TimeWindow window, Trigger.TriggerContext ctx) throws Exception {
        logger.info("On Processing onEventTime: {}, start: {}, end: {}", window.maxTimestamp(), window.getStart(), window.getEnd());
        return TriggerResult.CONTINUE;
    }

    public TriggerResult onProcessingTime ( long time, TimeWindow window, Trigger.TriggerContext ctx){
        logger.info("On Processing time: {}, start: {}, end: {}", window.maxTimestamp(), window.getStart(), window.getEnd());
        return TriggerResult.FIRE;
    }

    public void clear (org.apache.flink.streaming.api.windowing.windows.TimeWindow window, Trigger.TriggerContext ctx) throws
    Exception {
        logger.info("On Processing clear: {}, start: {}, end: {}", window.maxTimestamp(), window.getStart(), window.getEnd());
        ctx.deleteProcessingTimeTimer(window.maxTimestamp());
    }

    public boolean canMerge () {
        return true;
    }

    public void onMerge (org.apache.flink.streaming.api.windowing.windows.TimeWindow window, Trigger.OnMergeContext ctx)
    {
        long windowMaxTimestamp = window.maxTimestamp();
        if (windowMaxTimestamp > ctx.getCurrentProcessingTime()) {
            ctx.registerProcessingTimeTimer(windowMaxTimestamp);
        }

    }

    public String toString () {
        return "ProcessingTimeTrigger()";
    }
}