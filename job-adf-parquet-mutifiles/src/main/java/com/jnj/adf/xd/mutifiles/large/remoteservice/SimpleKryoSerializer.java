package com.jnj.adf.xd.mutifiles.large.remoteservice;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.Output;
/*import com.gemstone.gemfire.internal.ClassPathLoader;*/
import org.apache.geode.internal.ClassPathLoader;
import com.jnj.adf.grid.common.ADFException;

/**
 * Created by dyang39 on 6/30/2017.
 */
public class SimpleKryoSerializer {
    private SimpleKryoSerializer() {
        throw new IllegalStateException("Utility class");
    }

    public static final int DEFAULT_POOL_SIZE = 16;
    public static final int BUFF_SIZE = 4096;
    public static final int MAX_BUFF_SIZE = 655370000;

    private static final ThreadLocal<Kryo> tl_basic_kryo = ThreadLocal.withInitial(Kryo::new);

    public static final Kryo findKryo() {
        Kryo kryo = tl_basic_kryo.get();
        ClassLoader old = kryo.getClassLoader();
        ClassLoader newLoader = ClassPathLoader.getLatestAsClassLoader();
        if (!old.equals(newLoader)) {
            kryo.getClassResolver().reset();
            kryo.setClassLoader(ClassPathLoader.getLatestAsClassLoader());
        }
        return kryo;
    }

    public static final <T> byte[] toBytes(T value) {
        return toBytes(findKryo(), value);
    }

    public static final <T> byte[] toBytes(Kryo kryo, T value) {
        ByteBufferOutput out = new ByteBufferOutput(BUFF_SIZE, MAX_BUFF_SIZE);
        try {
            return toBytes(kryo, out, value);
        } catch (Exception e) {
            throw new ADFException(e);
        } finally {
            out.release();
            out.close();
        }
    }

    public static final <T> byte[] toBytes(Kryo kryo, Output out, T value) {
        try {
            kryo.writeObject(out, value);
            out.flush();
            return out.toBytes();
        } catch (Exception e) {
            throw new ADFException(e);
        }
    }

    public static final <T> T toObject(byte[] bytes, Class<T> tClass) {
        Kryo kryo = findKryo();
        return toObject(kryo, bytes, tClass);
    }

    public static final <T> T toObject(Kryo kryo, byte[] bytes, Class<T> tClass) {
        ByteBufferInput input = null;
        try {
            input = createInput(bytes);
            return kryo.readObject(input, tClass);
        } catch (Exception e) {
            throw new ADFException(e);
        } finally {
            if (input != null) {
                input.release();
            }
        }
    }

    private static ByteBufferInput createInput(byte[] bytes) {
        return new ByteBufferInput(bytes);
    }
}
