package top.javinjunfeng.cdh.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

/**
 * Hadoop HDFS Java API 操作
 * @author javinjunfeng
 * @date 2018/7/26
 */
public class HDFSApp {

    public static final String HDFS_PATH = "hdfs://hadoop.javinjunfeng.top:8020";
    public static final String USER = "katarina";

    FileSystem fileSystem = null;
    Configuration configuration = null;


    /**
     * 创建HDFS目录
     * @throws Exception
     */
    @Test
    public void mkdir() throws Exception{
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }


    /**
     * 创建文件
     * @throws Exception
     */
    @Test
    public void create() throws Exception{
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/hdfsapi/test/a.txt"));
        fsDataOutputStream.write("hello hadoop".getBytes());
        fsDataOutputStream.flush();
        fsDataOutputStream.close();
    }


    /**
     * 查看文件内容
     * @throws Exception
     */
    @Test
    public void cat() throws Exception{
        FSDataInputStream open = fileSystem.open(new Path("/hdfsapi/test/a.txt"));
        IOUtils.copyBytes(open,System.out,1024);
        open.close();
    }


    /**
     * 重命名
     * @throws Exception
     */
    @Test
    public void rename() throws Exception{
        Path oldPath = new Path("/hdfsapi/test/a.txt");
        Path newPath = new Path("/hdfsapi/test/b.txt");
        boolean rename = fileSystem.rename(oldPath, newPath);
    }


    /**
     * 上传文件到HDFS
     * @throws Exception
     */
    @Test
    public void copyFromLocalFile() throws Exception{
        Path localPath = new Path("/Users/katarina/IDEA激活.txt");
        Path hdfsPath = new Path("/hdfsapi/test");
        fileSystem.copyFromLocalFile(localPath, hdfsPath);

    }


    /**
     * 上传文件到HDFS
     * @throws Exception
     */
    @Test
    public void copyFromLocalFileWithProgress() throws Exception{
        InputStream in = new BufferedInputStream(new FileInputStream(new File("/Users/katarina/Downloads/高性能MySQL 第3版 中文 .pdf")));
        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/test/高性能MySQL1.pdf"), new Progressable() {
            public void progress() {
                System.out.print("."); // 带进度提醒信息
            }
        });
        IOUtils.copyBytes(in,out,4096);
    }


    /**
     * 下载HDFS文件
     * @throws Exception
     */
    @Test
    public void copyToLocalFile() throws Exception{
        Path localPath = new Path("/Users/katarina/IDEA激活2.txt");
        Path hdfsPath = new Path("/hdfsapi/test/IDEA激活.txt");
        fileSystem.copyToLocalFile(hdfsPath,localPath);

    }


    /**
     * 下载HDFS文件
     * @throws Exception
     */
    @Test
    public void listFiles() throws Exception{
        Path hdfsPath = new Path("/hdfsapi/test");
        FileStatus[] fileStatuses = fileSystem.listStatus(hdfsPath);
        for (FileStatus fileStatus : fileStatuses){
            String isDir = fileStatus.isDirectory() ? "文件夹" : "文件";
            short replication = fileStatus.getReplication();
            long len = fileStatus.getLen();
            String path = fileStatus.getPath().toString();
            System.out.println(isDir + " | " + replication + " | " + len + " | " + path);
        }

    }


    /**
     * 删除HDFS文件
     * @throws Exception
     */
    @Test
    public void delete() throws Exception{
        Path hdfsPath = new Path("/hdfsapi/test/高性能MySQL1.pdf");
        fileSystem.delete(hdfsPath,true);
    }




    @Before
    public void setUp() throws Exception{
        System.out.println("HDFSApp.setUp");
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH),configuration,USER);

    }


    @After
    public void tearDown() throws Exception{
        fileSystem = null;
        configuration = null;
        System.out.println("HDFSApp.tearDown");

    }
}
