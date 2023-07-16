package com.jnj.adf.xd.mutifiles.util;

public class CommonUtils {
    private CommonUtils() {
        throw new IllegalStateException("Utility class");
    }

    public static String getFileNameFromRegionName(String regionName) {
        return getRegionPath(regionName, false).replaceAll("/", "_");
    }

    public static String getPathWithRegionName(String path, String regionName) {
        return getPathWithRegionName(path, regionName, false);
    }

    public static String getPathWithRegionName(String path, String regionName, boolean last) {
        String realPath = "";
        regionName = getRegionPath(regionName, false);

        if (path.charAt(path.length() - 1) != '/') {
            realPath = path + '/' + regionName;
        } else {
            realPath = path + regionName;
        }
        if (last) {
            realPath += "/";
        }
        return realPath;
    }

    public static String getRegionPath(String regionName) {
        return getRegionPath(regionName, true);
    }

    public static String getRegionPath(String regionName, boolean start) {
        regionName = regionName.startsWith("/") ? regionName : regionName + "/";
        regionName = regionName.endsWith("/") ? regionName.substring(0, regionName.length() - 1) : regionName;
        if (!start) {
            regionName = regionName.substring(1);
        }
        return regionName;
    }
}
