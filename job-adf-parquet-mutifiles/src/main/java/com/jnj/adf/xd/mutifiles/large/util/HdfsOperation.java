package com.jnj.adf.xd.mutifiles.large.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;


/**
 * Simple Driver to read/write to hdfs.  		// It uses Kerberos, no SSL.		Fully tested.
 * <p>
 * Notes:  Remember to set VM argument:  -Djava.security.krb5.conf=krb5.conf
 * To debug:  -Dsun.security.krb5.debug=true
 * <p>
 * https://gist.github.com/ashrithr/f7899fdfd36ee800f151
 * <p>
 * Tested:
 * add  test.csv hdfs:/dev/edl/sc/sce2e/str/sce2e_stg/t3
 * read /dev/edl/sc/sce2e/str/sce2e_stg/t3/test.csv								This line is the same as the next 2 lines!!!
 * read hdfs:/dev/edl/sc/sce2e/str/sce2e_stg/t3/test.csv
 * read hdfs://itsusraedld01.jnj.com/dev/edl/sc/sce2e/str/sce2e_stg/t3/test.csv
 */
public class HdfsOperation {
    private static final Logger logger = LoggerFactory.getLogger(HdfsOperation.class);

    private HdfsOperation() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * read a file from hdfs
     *
     * @throws Exception
     */
    public static void readFile(String file, Configuration conf) throws IOException {
        Path path = new Path(file);
        String filename = file.substring(file.lastIndexOf('/') + 1, file.length());

        try (FileSystem fileSystem = FileSystem.newInstance(conf);
             FSDataInputStream in = fileSystem.open(path);
             OutputStream out = new BufferedOutputStream(new FileOutputStream(new File(filename)));) {

            if (!fileSystem.exists(path)) {
                logger.error("File {}  does not exists", file);
                return;
            }

            byte[] b = new byte[1024];
            int numBytes = 0;
            while ((numBytes = in.read(b)) > 0) {
                out.write(b, 0, numBytes);
            }

            logger.info("Output file {} is created in your local drive.", filename);
        } catch (Exception e) {
            logger.error("Read file error.", e);
            throw e;
        }
    }

    /**
     * delete a directory in hdfs		// need testing
     */
    public static void deleteFile(String file, Configuration conf) throws IOException {
        try (FileSystem fileSystem = FileSystem.newInstance(conf);) {

            Path path = new Path(file);
            if (!fileSystem.exists(path)) {
                logger.error("File {}  does not exists.", file);
                return;
            }

            fileSystem.delete(new Path(file), true);
        } catch (Exception e) {
            logger.error("Delete file error.", e);
            throw e;
        }
    }

    /**
     * create directory in hdfs			// need testing
     */
    public static void mkdir(String dir, Configuration conf) throws IOException {
        try (FileSystem fileSystem = FileSystem.newInstance(conf);) {
            Path path = new Path(dir);
            if (fileSystem.exists(path)) {
                logger.error("Dir {} already exists.", dir);
                return;
            }

            fileSystem.mkdirs(path);
        } catch (Exception e) {
            logger.error("Mkdir error.", e);
            throw e;
        }
    }

    /**
     * copy a file from local filesystem to hdfs	// need testing
     */
    public static void addFile(String source, String dest, Configuration conf) throws IOException {
        source = source.replaceAll("\\\\", "/");
        dest = dest.replaceAll("\\\\", "/");
        String filename = source.substring(source.lastIndexOf('/') + 1, source.length());
        if (dest.charAt(dest.length() - 1) != '/') {
            dest = dest + '/' + filename;
        } else {
            dest = dest + filename;
        }
        Path path = new Path(dest);

        try (FileSystem fileSystem = FileSystem.newInstance(conf);
             InputStream in = new BufferedInputStream(new FileInputStream(new File(source)));) {
            // Create the destination path including the filename.
            if (fileSystem.exists(path)) {
                fileSystem.delete(path, true);
                logger.info("File {} already exists and deleted.", dest);
            }

            try(FSDataOutputStream out = fileSystem.create(path);){
                // Create a new file and write data to it.
                byte[] b = new byte[1024];
                int numBytes = 0;
                while ((numBytes = in.read(b)) > 0) {
                    out.write(b, 0, numBytes);
                }
            }
        } catch (Exception e) {
            logger.error("ADD error.", e);
            throw e;
        }

        logger.warn("File {} added to HDFS:{}.", filename, dest);
    }

    public static Configuration kerberosLogin(String defaultFS, String kPrincipal, String keyPath, String hdfsConfigPath) throws IOException {
        String krbPath = getKrbPath(hdfsConfigPath);
        System.setProperty("java.security.krb5.conf", krbPath);

        Configuration conf = new Configuration();
        addConf(conf, hdfsConfigPath);
        conf.set("fs.defaultFS", defaultFS);
        conf.set("hadoop.security.authentication", "Kerberos");

        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(kPrincipal, keyPath);
        return conf;
    }

    private static String getKrbPath(String hdfsConfigPath) {
        File configDir = new File(hdfsConfigPath);
        if (configDir.isDirectory()) {
            File[] files = configDir.listFiles(file -> file.getName().toLowerCase().endsWith(".conf"));
            if (files.length > 0) {
                for (File file : files) {
                    if (file.getName().equalsIgnoreCase("krb5.conf")) {
                        return file.getAbsolutePath();
                    }
                }
                return files[0].getAbsolutePath();
            }
        }
        return null;
    }

    private static void addConf(Configuration conf, String path) throws FileNotFoundException {
        File file = new File(path);
        if (file.isDirectory()) {
            for (File tmp : file.listFiles()) {
                if (tmp.getName().toLowerCase().endsWith(".xml")) {
                    conf.addResource(new FileInputStream(tmp));
                }
            }
        } else {
            conf.addResource(new FileInputStream(file));
        }
    }
}
