package com.sludev.commons.vfs2.provider.minio;

import com.sludev.commons.vfs2.provider.s3.*;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.UploadObjectArgs;
import io.minio.errors.MinioException;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.Selectors;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.provider.local.DefaultLocalFileProvider;
import org.junit.*;
import org.junit.rules.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Properties;

public class MinIOClientTest {
    private static final Logger log = LoggerFactory.getLogger(SS3FileProviderTest.class);

    private Properties testProperties;

    public MinIOClientTest()
    {
    }



    @Before
    public void setUp()
    {

        /**
         * Get the current test properties from a file so we don't hard-code
         * in our source code.
         */
        testProperties = SS3TestProperties.GetProperties();

        try
        {
            /**
             * Setup the remote folders for testing
             */
            //uploadFileSetup02();
        }
        catch (Exception ex)
        {
            log.debug("Error setting up remote folder structure.  Have you set the test001.properties file?", ex);
        }
    }

    @BeforeClass
    public static void setUpClass()
    {
    }

    @AfterClass
    public static void tearDownClass()
    {
    }

    @After
    public void tearDown() throws Exception
    {
        //removeFileSetup02();
    }

    /**
     * Upload a single file to the test bucket.
     * @throws java.lang.Exception
     */
    @Test
    public void A001_uploadFile() throws Exception
    {
          String endPoint = "http://127.0.0.1:9000";
        final  String accessKey = "test";
        final  String secretKey = "test1234";
        final  String bucketName = "bucket";
        final  String localFileFolder = "C:\\Users\\DELL\\Desktop\\minioApi\\";
        String currFileNameStr;


        File temp = File.createTempFile("uploadFile01", ".tmp");
        try(FileWriter fw = new FileWriter(temp))
        {
            BufferedWriter bw = new BufferedWriter(fw);
            bw.append("testing...");
            bw.flush();
        }

        MinIOFileProvider currMinIO = new MinIOFileProvider();

        // Optional set endpoint
        //currSS3.setEndpoint(currHost);

        // Optional set region
        //currSS3.setRegion(currRegion);

        DefaultFileSystemManager currMan = new DefaultFileSystemManager();
        currMan.addProvider("minio", currMinIO);
        currMan.addProvider("file", new DefaultLocalFileProvider());
        currMan.init();

        StaticUserAuthenticator auth = new StaticUserAuthenticator("", accessKey, secretKey);
        FileSystemOptions opts = new FileSystemOptions();
        DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth);

        currFileNameStr = "test01.tmp";
        String currUriStr = String.format("%s://%s/%s/%s",
                SS3Constants.S3SCHEME, endPoint, bucketName, currFileNameStr);
        FileObject currFile = currMan.resolveFile(currUriStr, opts);
        FileObject currFile2 = currMan.resolveFile(
                String.format("file://%s", temp.getAbsolutePath()));

        currFile.copyFrom(currFile2, Selectors.SELECT_SELF);
        temp.delete();










    }
}
