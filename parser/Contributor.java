
import java.util.ArrayList;
import java.util.List;
import java.io.*;
import java.util.TreeMap;

/**
 * Contributors
 */
public class Contributor {
    /**
     * Id of this contributor
     */
    private String id;

    /**
     * Username of this contributor
     */
    private String username;

    /**
     * Container of all contributors in thif file
     */
    static TreeMap<String, List<String>> allContributors = new TreeMap<>();

    /**
     * List of bots to be filtered
     */
    private static List<String> bots = new ArrayList<>();

    /**
     * Read the bots list in
     */
    static {
        File f = new File("/Users/Li/Downloads/bots_list.txt");
        try(BufferedReader br = new BufferedReader(new FileReader(f))){
            String line;
            while((line = br.readLine())!=null){
                bots.add(line);
                br.readLine();
                br.readLine();
                br.readLine();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    Contributor(){
        setId(null);
        setUsername(null);
    }

    void setId(String id){
        this.id = id;
    }

    String getId(){
        return this.id;
    }

    void setUsername(String username){
        this.username = username;
    }

    boolean isBot(){
        return this.username != null && bots.contains(this.username);
    }

    /**
     * Records this contributor into the contributor container
     */
    void record(){
        List<String> l;
        if(!this.isBot()){
            if(allContributors.containsKey(this.getId())){
                l = allContributors.get(this.id);
            }else{
                l = new ArrayList<>();
            }
            if(!l.contains(this.username)){
                l.add(this.username);
                allContributors.put(this.id, l);
            }
        }
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        return sb.append(this.id).append(',').append(this.username).append('\n').toString();
    }

    public static void main(String[] args){


    }
}

