package com.jnj.adf.xd.mutifiles.util;

import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.util.UnsafeUtil;

import java.nio.ByteBuffer;

public class ADFByteBufferOutput extends ByteBufferOutput
{
  protected ByteBuffer oldNiobuffer = null;


  public ADFByteBufferOutput(int bufferSize, int maxBufferSize)
  {
    super(bufferSize,maxBufferSize);
  }

  @Override
  public boolean require(int required)
  {
    oldNiobuffer = niobuffer;
    boolean assign = super.require(required);
    if(assign && oldNiobuffer !=null)
    {
      oldNiobuffer.clear();
      UnsafeUtil.releaseBuffer(oldNiobuffer);
      oldNiobuffer = null;
    }
    return assign;
  }
}
