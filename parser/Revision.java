/**
 * Created by Li on 12/4/16.
 */
public class Revision {
    /**
     * Revision's id
     */
    private String id;
    /**
     * namespace of this page
     */
    private int ns;
    /**
     * Id of this page
     */
    private int pageID;
    /**
     * Time when this edit made
     */
    private String timestamp;
    /**
     * Contributor of this revision
     */
    private Contributor contributor;
    /**
     * Whether this is a minor edit or not
     */
    private boolean minor;
    /**
     * Length of this revision
     */
    private String length;

    public Revision(){
        setId(null);
        setTimestamp(null);
        ns = 0;
        pageID = 0;
        setLength(null);
        setContributor(null);
    }

    public void setId(String id){
        this.id = id;
    }

    public void setNs(int ns){
        this.ns = ns;
    }

    public void setPageID(int pageID){
        this.pageID = pageID;
    }

    public void setContributor(Contributor contributor){
        this.contributor = contributor;
    }

    Contributor getContributor(){
        return this.contributor;
    }

    public void setTimestamp(String timestamp){
        this.timestamp = timestamp;
    }

    void setMinor(){
        this.minor = true;
    }

    boolean isMinor(){
        return this.minor;
    }

    public void setLength(String length){
        this.length = length;
    }

    @Override
    public String toString(){
        if(this.ns != 0) return "";
        final char DELIMITER = ',';
        StringBuilder sb = new StringBuilder();
        sb.append(this.id).append(DELIMITER).append(this.pageID)
                .append(DELIMITER).append(this.contributor.getId()).append(DELIMITER)
                .append(this.length).append(DELIMITER).append(this.timestamp).append('\n');
        return sb.toString();
    }

}
