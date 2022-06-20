import java.io.Serializable;

public class Word implements Serializable {
    private String word;

    Word(String word) {
        this.word = word;
    }

    public String getWord() {
        return this.word;
    }
}
