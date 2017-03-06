import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;
/**
 * Class contain for revision list of pages
 */
public class RevisionList {
    /**
     * The page id of this revisionlist's page
     */
    private int id;
    /**
     * The list contains all revisions of this page
     */
    private List<Revision> list;



    RevisionList(){
        list = new ArrayList<>();
    }

    void setId(int id){
        this.id = id;
    }

    /**
     * Add a revision record in the list
     * @param revision
     */
    void addRevision(Revision revision){
        this.list.add(revision);
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append(this.id).append(",");
        String id;
        String lastId = null;
        Contributor contributor;
        String prefix = "";
        for( Revision r : this.list){
            if(r != null && !r.isMinor() && (contributor = r.getContributor())!= null && !contributor.isBot()){
                id = contributor.getId();
                if(lastId == null || !lastId.equals(id)) sb.append(prefix).append(id);
                lastId = id;
                prefix = "|";
            }
        }
        return sb.append('\n').toString();
    }

}
