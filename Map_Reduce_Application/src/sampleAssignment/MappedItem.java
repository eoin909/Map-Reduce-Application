package sampleAssignment;

public class MappedItem {
    private final String word;
    private final String file;

    MappedItem(String entry, String file) {
        this.word = entry;
        this.file = file;
    }

    String getWord() {
        return word;
    }

    String getFile() {
        return file;
    }

    @Override
    public String toString() {
        return "[\"" + word + "\",\"" + file + "\"]";
    }
}
