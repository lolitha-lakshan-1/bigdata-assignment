public class Result {

    private String flinkJobId;
    private long windowEndMillis;
    private long rawEventCount;
    private long hashtagEventCount;

    public Result() {
    }

    public Result(
            String flinkJobId,
            long windowEndMillis,
            long rawEventCount,
            long hashtagEventCount
    ) {
        this.flinkJobId = flinkJobId;
        this.windowEndMillis = windowEndMillis;
        this.rawEventCount = rawEventCount;
        this.hashtagEventCount = hashtagEventCount;
    }

    public String getFlinkJobId() {
        return flinkJobId;
    }

    public void setFlinkJobId(String flinkJobId) {
        this.flinkJobId = flinkJobId;
    }

    public long getWindowEndMillis() {
        return windowEndMillis;
    }

    public void setWindowEndMillis(long windowEndMillis) {
        this.windowEndMillis = windowEndMillis;
    }

    public long getRawEventCount() {
        return rawEventCount;
    }

    public void setRawEventCount(long rawEventCount) {
        this.rawEventCount = rawEventCount;
    }

    public long getHashtagEventCount() {
        return hashtagEventCount;
    }

    public void setHashtagEventCount(long hashtagEventCount) {
        this.hashtagEventCount = hashtagEventCount;
    }
}
