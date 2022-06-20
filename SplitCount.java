import java.io.Serializable;

public class SplitCount implements Serializable {
    private int totalSplits;

    SplitCount() {}
    SplitCount(int totalSplits) {
        this.totalSplits = totalSplits;
    }

    public int getSplitCount() {
        return this.totalSplits;
    }

}
