import java.io.Serializable;

public class Split implements Serializable {

    public Split() {};

    private static final long serialVersionUID = 12345L;

    private String destinationDirectory;
    private String sender;
    private String fileName;
    private byte[] fileData;
    private String status;

    public void setDestinationDirectory(String destinationDirectory) {
        this.destinationDirectory = destinationDirectory;
    }

    public String getDestinationDirectory() {
        return this.destinationDirectory;
    }

    public void setSender(String sender) {
        this.sender = sender;
    }

    public String getSender() {
        return this.sender;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getFileName() {
        return this.fileName;
    } 

    public void setFileData(byte[] fileData) {
        this.fileData = fileData;
    }

    public byte[] getFileData() {
        return this.fileData;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getStatus() {
        return this.status;
    }
}
