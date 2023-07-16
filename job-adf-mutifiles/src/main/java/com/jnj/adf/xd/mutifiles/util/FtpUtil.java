package com.jnj.adf.xd.mutifiles.util;

import com.jcraft.jsch.*;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jnj.adf.grid.common.ADFException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class FtpUtil {
    private static final Logger logger = LoggerFactory.getLogger(FtpUtil.class);

    private String ip;
    private int port;
    private String userName;
    private String password;
    private boolean isConnect = false;

    private Session session;

    public FtpUtil(String ip, int port, String userName, String password) {
        logger.info("connectting to {} success", ip);

        this.ip = ip;
        this.port = port;
        this.userName = userName;
        this.password = password;

        try {
            connect();
            isConnect = true;
            logger.info("connect to {} success", ip);
        } catch (Exception e) {
            logger.error("connect failed", e);
        }
    }

    private void connect() throws JSchException {
        JSch jsch = new JSch();
        session = jsch.getSession(userName, ip, port);
        session.setPassword(password);
        Properties config = new Properties();
        config.put("StrictHostKeyChecking", "no");
        config.put("PreferredAuthentications", "publickey,keyboard-interactive,password");
        session.setConfig(config);
        session.connect();
    }

    public void uploadFile(String file, String path) {
        uploadFile(new File(file), path);
    }

    public void uploadFile(File file, String path) {
        logger.info("uploading file {} to {} ", file.getAbsoluteFile(), path);
        try {
            if (file.exists() && file.isFile()) {
                ChannelSftp channel = newChannel();
                String destination = getRealdestinationPath(file.getName(), path);
                mkdirs(channel, path);
                channel.put(new FileInputStream(file), destination);
                channel.disconnect();
                logger.info("upload file {} to {} success", file.getAbsoluteFile(), destination);
            } else {
                throw new ADFException(file.getAbsolutePath() + " not exists or is not a file.");
            }
        } catch (Exception e) {
            throw new ADFException("Upload file error", e);
        }
    }

    @SuppressWarnings("unchecked")
    private void mkdirs(ChannelSftp channel, String path) throws SftpException {
        logger.info("mkdirs {} ", path);
        String directory = "";
        if (path.endsWith("/")) {
            directory = path.substring(0, path.length() - 1);
        } else {
            directory = path;
        }
        if (directory.startsWith("/")) directory = directory.substring(1);
        String[] directorys = directory.split("/");
        String tmp = "/";
        String pre;
        for (int i = 0; i < directorys.length; i++) {
            String dy = directorys[i];
            pre = tmp;
            tmp = pre + dy + "/";

            List<LsEntry> list = new ArrayList<>(channel.ls(pre));
            boolean exist = false;
            for (LsEntry lsEntry : list) {
                if (!lsEntry.getFilename().startsWith(".") && lsEntry.getFilename().equals(dy)) {
                    exist = true;
                    break;
                }
            }

            if (!exist) {
                channel.cd(pre);
                logger.info("mkdir pre: {} directory:{}", pre, dy);
                channel.mkdir(dy);
            }
        }
    }

    private String getRealdestinationPath(String name, String path) {
        if (path.endsWith("/")) {
            return path + name;
        }
        return path + "/" + name;
    }

    private ChannelSftp newChannel() throws JSchException {
        ChannelSftp channel = (ChannelSftp) session.openChannel("sftp");
        channel.connect();
        return channel;
    }

    public void close() {
        if (session != null)
            session.disconnect();
    }

    public boolean isConnect() {
        return isConnect;
    }
}
