/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sludev.commons.vfs2.provider.minio;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.sludev.commons.vfs2.provider.s3.SS3FileNameParser;
import com.sludev.commons.vfs2.provider.s3.SS3FileSystem;
import com.sludev.commons.vfs2.provider.s3.SS3FileSystemConfigBuilder;
import io.minio.MinioClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.AbstractOriginatingFileProvider;
import org.apache.commons.vfs2.provider.GenericFileName;
import org.apache.commons.vfs2.util.UserAuthenticatorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * The main provider class in the Simple S3 Commons VFS provider.
 * 
 * This class can be declared and passed to the current File-system manager 
 * 
 * E.g....
 * <pre><code>
 * SS3FileProvider currSS3 = new SS3FileProvider();
 *
 * //Optional set endpoint
 * currSS3.setEndpoint(currHost);
 *
 * // Optional set region
 * currSS3.setRegion(currRegion);
 * 
 * DefaultFileSystemManager currMan = new DefaultFileSystemManager();
 * currMan.addProvider(SS3Constants.S3SCHEME, currSS3);
 * currMan.init(); 
 *
 * StaticUserAuthenticator auth = new StaticUserAuthenticator("", currAccountStr, currKey);
 * FileSystemOptions opts = new FileSystemOptions(); 
 * DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth); 
 * </code></pre>
 * 
 * @author Kervin Pierre
 */
public class MinIOFileProvider
                  extends AbstractOriginatingFileProvider
{
    private static final Logger log = LoggerFactory.getLogger(MinIOFileProvider.class);

    private static final FileSystemOptions DEFAULT_OPTIONS = new FileSystemOptions();

    public static final UserAuthenticationData.Type[] AUTHENTICATOR_TYPES = new UserAuthenticationData.Type[]
        {
            UserAuthenticationData.USERNAME, UserAuthenticationData.PASSWORD
        };

    private String endpoint;
    private Regions region;

    static final Collection<Capability> capabilities = Collections.unmodifiableCollection(Arrays.asList(new Capability[]
            {
                    Capability.GET_TYPE,
                    Capability.READ_CONTENT,
                    Capability.APPEND_CONTENT,
                    Capability.URI,
                    Capability.ATTRIBUTES,
                    Capability.RANDOM_ACCESS_READ,
                    Capability.DIRECTORY_READ_CONTENT,
                    Capability.LIST_CHILDREN,
                    Capability.LAST_MODIFIED,
                    Capability.GET_LAST_MODIFIED,
                    Capability.CREATE,
                    Capability.DELETE
            }));

    /**
     * Creates a new FileProvider object.
     */
    public MinIOFileProvider()
    {
        super();
        setFileNameParser(SS3FileNameParser.getInstance());
        endpoint = null;
    }
    
    /**
     * In the case that we are not sent FileSystemOptions object, we need to have
     * one handy.
     * 
     * @return 
     */
    public FileSystemOptions getDefaultFileSystemOptions()
    {
        return DEFAULT_OPTIONS;
    }
    
    /**
     * Create FileSystem event hook
     * 
     * @param rootName
     * @param fileSystemOptions
     * @return
     * @throws FileSystemException 
     */
    @Override
    protected FileSystem doCreateFileSystem(FileName rootName, FileSystemOptions fileSystemOptions) throws FileSystemException
    {
        MinIOFileSystem fileSystem = null;
        GenericFileName genRootName = (GenericFileName)rootName;

        MinioClient.Builder storageCreds;
        MinioClient client;
        FileSystemOptions currFSO;
        UserAuthenticator ua;
        
        if( fileSystemOptions == null )
        {
            currFSO = getDefaultFileSystemOptions();
            ua = SS3FileSystemConfigBuilder.getInstance().getUserAuthenticator(currFSO);
        }
        else
        {
            currFSO = fileSystemOptions;
            ua = DefaultFileSystemConfigBuilder.getInstance().getUserAuthenticator(currFSO);
        }
        
        UserAuthenticationData authData = null;
        try
        {
            authData = ua.requestAuthentication(AUTHENTICATOR_TYPES);
            
            String currAcct = UserAuthenticatorUtils.toString(UserAuthenticatorUtils.getData(authData,
                    UserAuthenticationData.USERNAME, UserAuthenticatorUtils.toChar(genRootName.getUserName())));
            
            String currKey =  UserAuthenticatorUtils.toString(UserAuthenticatorUtils.getData(authData,
                    UserAuthenticationData.PASSWORD, UserAuthenticatorUtils.toChar(genRootName.getPassword())));
        
           /* //storageCreds = new BasicAWSCredentials(currAcct, currKey);
            storageCreds = new MinioClient.Builder().credentials(currAcct, currKey);

                    client = new MinioClient.Builder().credentials(currAcct, currKey).build();
            
            if( StringUtils.isNoneBlank(endpoint) )
            {
                //client.setEndpoint(endpoint);
                client.endpoint()
            }*/
            if( StringUtils.isNoneBlank(endpoint) && region != null )
            {
                client = MinioClient.builder().endpoint(endpoint).credentials(currAcct,currKey).region(region.getName()).build();
                fileSystem = new MinIOFileSystem(genRootName, client, fileSystemOptions);
            }
            

        }
        finally
        {
            UserAuthenticatorUtils.cleanup(authData);
        }
        
        return fileSystem;
    }

    /**
     * Returns the provider's capabilities.
     * 
     * @return 
     */
    @Override
    public Collection<Capability> getCapabilities()
    {
        return capabilities;
    }
    
    /**
     * Set the S3 endpoint we should use.  This needs to be done before init() is called.
     * 
     * @param ep 
     */
    public void setEndpoint(String ep)
    {
        endpoint = ep;
    }

    /**
     * Returns the currently set region.
     * 
     * @return 
     */
    public Regions getRegion()
    {
        return region;
    }

    /**
     * Set the S3 Region we should use for this provider.
     * 
     * @param region 
     */
    public void setRegion(Regions region)
    {
        this.region = region;
    }
    
    /**
     * Sets the AWS Region but first converts from a String.
     * @param r 
     */
    public void setRegion(String r)
    {
        this.region = Regions.fromName(r);
    }
}
