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

import com.sludev.commons.vfs2.provider.s3.SS3FileObject;
import io.minio.errors.*;

import java.io.*;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

/**
 * Wrap an output stream for AWS stream upload.  Which unfortunately uses an
 * InputStream.
 * 
 * This OutputStream buffers all data to a local file then automatically uploads
 * it to Amazon S3 after <code>close()</code> is called.
 * 
 * @author kervin
 */
public final class MinIOOutputStream extends OutputStream
{
    private final File tempFile;
    private final OutputStream tempFileStream;
    private final MinIOFileObject fileObject;

    public File getTempFile()
    {
        return tempFile;
    }

    public MinIOOutputStream(MinIOFileObject fo) throws IOException
    {
        super();
        
        tempFile = File.createTempFile("bin", "bin");
        tempFile.deleteOnExit();
        
        tempFileStream = new BufferedOutputStream(new FileOutputStream(tempFile));
        
        fileObject = fo;
    }

    @Override
    public void write(int i) throws IOException
    {
        tempFileStream.write(i);
    }

    @Override
    public void close() throws IOException
    {
        tempFileStream.close();
        
        // Upload tempFile
        try {
            fileObject.putObject(tempFile);
        } catch (ServerException e) {
            e.printStackTrace();
        } catch (InvalidBucketNameException e) {
            e.printStackTrace();
        } catch (InsufficientDataException e) {
            e.printStackTrace();
        } catch (ErrorResponseException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (InvalidKeyException e) {
            e.printStackTrace();
        } catch (InvalidResponseException e) {
            e.printStackTrace();
        } catch (XmlParserException e) {
            e.printStackTrace();
        } catch (InternalException e) {
            e.printStackTrace();
        }
        tempFile.delete();
    }

    @Override
    public void flush() throws IOException
    {
        tempFileStream.flush();
    }

    @Override
    public void write(byte[] bytes, int i, int i1) throws IOException
    {
        tempFileStream.write(bytes, i, i1);
    }

    @Override
    public void write(byte[] bytes) throws IOException
    {
        tempFileStream.write(bytes);
    }

    @Override
    public String toString()
    {
        return super.toString(); 
    }

    @Override
    public boolean equals(Object o)
    {
        return super.equals(o); 
    }

    @Override
    public int hashCode()
    {
        return super.hashCode(); 
    }
}
