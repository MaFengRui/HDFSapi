import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsTest {
    FileSystem fileSystem = null;


     //
     public    void HdfsConnection()throws Exception{
         Configuration con = new Configuration();
         fileSystem = FileSystem.newInstance(con);

         fileSystem.mkdirs(new Path("/dd"));

         fileSystem.close();
     }

     public void TestPut(String inpath,String outpath){
         

     }


    public static void main(String[] args) throws Exception{



    }
}
