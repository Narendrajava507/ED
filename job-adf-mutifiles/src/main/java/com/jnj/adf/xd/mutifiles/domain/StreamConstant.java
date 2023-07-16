package com.jnj.adf.xd.mutifiles.domain;

/**
 * Created by dyang39 on 7/4/2017.
 */
public final class StreamConstant {
    private StreamConstant() {
        throw new IllegalStateException("Utility class");
    }

    public static final byte TYPE_CHUNK = 0x30;
    public static final byte DATA_CHUNK = 0x31;
    public static final byte ERROR_CHUNK = 0x32;
    public static final byte COLUMN_CHUNK = 0x33;
    public static final byte SIZE_CHUNK = 0x34;
    public static final byte SER_DATA = 0x41;
    public static final byte UNSER_DATA = 0x42;
    public static final byte BYTEARR_DATA = 0x43;
    public static final byte BYTEARR_ARR_DATA = 0x44;
    public static final byte BYTEARR_FK = 0x45;
    public static final byte BYTEARR_OTHER = 0x46;
    public static final byte BYTEARR_BUCKETID = 0x47;
}
