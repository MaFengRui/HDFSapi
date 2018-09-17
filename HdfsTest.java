
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.DataInputStream;
import java.io.FileOutputStream;
import java.io.FileSytem;
import java.io.IOException;

/**
 * hdfs的简单操作
 */
public class HdfsTest {
    static  FileSystem fileSystem = null;


     //简单的测试
     public    void HdfsConnection()throws Exception{
         Configuration con = new Configuration();
         fileSystem = FileSystem.newInstance(con);
         fileSystem.mkdirs(new Path("/dd"));
         fileSystem.close();

     }
        //简单的上传文件
     public static  void TestPut() {
         Configuration configuration = new Configuration();
         try {
             fileSystem = FileSystem.newInstance(configuration);
             fileSystem.copyFromLocalFile(new Path("/usr/share/dde-introduction/demo.mp4"), new Path("/test01/"));
         } catch (IOException e) {
             e.printStackTrace();
         }finally {
             try {
                 fileSystem.close();
                 configuration.clear();
             } catch (IOException e) {
                 e.printStackTrace();
             }

         }
     }

     //简单的下载文件

     public  static  void Testup(){
         Configuration configuration = new Configuration();
         try {
             fileSystem = FileSystem.newInstance(configuration);
             FSDataInputStream fsDataInputStream  = fileSystem.open(new Path("/test01/date.txt"));
             FileOutputStream fileOutputStream = new FileOutputStream("/home/mafenrgui/Desktop/11.txt");
             IOUtils.copyBytes(fsDataInputStream,fileOutputStream,1024,true);

         } catch (IOException e) {

             e.printStackTrace();
         }finally {
             System.out.println("已完成");
         }

     }

     //简单的删除文件　
    public static void Testdelete(){
         Configuration configuration = new Configuration();
        try {
            fileSystem=FileSystem.newInstance(configuration);
            fileSystem.deleteOnExit(new Path("/test01/date.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                fileSystem.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


    }
    //简单的合并传输
    public  static void mergePut(){
        Configuration configuration = new Configuration();
        try {
            Path[] paths = new Path[2];
            paths[0]= new Path("/home/mafenrgui/Desktop/11.txt");
            paths[1]= new Path("/home/mafenrgui/Desktop/22.txt");
            Path pathdsc = new Path("/test01/");  //只能创建到目录
            fileSystem = FileSystem.newInstance(configuration);
            fileSystem.copyFromLocalFile(true,true,paths,pathdsc);

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                fileSystem.close();
                configuration.clear();
                System.out.println("已完成");
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

    }

    //简单的查看文件
    public  static  void  CatFile(String path){
        Configuration configuration = new Configuration();
        try {
            fileSystem = FileSystem.newInstance(configuration);
           DataInputStream  dataInputStream =  fileSystem.open(new Path(path));
           byte[] bytes = new byte[20];
           int len =0;
          while ((len = dataInputStream.read(bytes) )!= -1) {
                String string = new String(bytes,0,len);
              System.out.println(string);
            }
            dataInputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                fileSystem.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }





    public static void main(String[] args) throws Exception{
//      TestPut();
//      Testup();
//      Testdelete();
        mergePut();
//        CatFile("/test01/date.txt");
    }
}
