
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowResultFunction
        extends ProcessWindowFunction<Event, Result, Integer, TimeWindow> {

    private final String targetHashtag;
    private final String flinkJobId;

    public WindowResultFunction(String targetHashtag, String flinkJobId) {
        this.targetHashtag = targetHashtag;
        this.flinkJobId = flinkJobId;
    }

    @Override
    public void process(
            Integer key,
            Context context,
            Iterable<Event> events,
            Collector<Result> out
    ) {
        long windowEndMillis = context.window().getEnd();

        long rawCount = 0;
        long hashtagCount = 0;

        for (Event event : events) {
            rawCount = rawCount + 1;
            if (event.containsHashtagIgnoreCase(targetHashtag)) {
                hashtagCount = hashtagCount + 1;
            }
        }

        out.collect(new Result(
                flinkJobId,
                windowEndMillis,
                rawCount,
                hashtagCount
        ));
    }

}
