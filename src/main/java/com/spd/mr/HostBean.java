package com.spd.mr;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class HostBean implements Writable {

    /**imei*/
    private String imei;

    /**url*/
    private String uri;

    /**次数**/
    private long count;

    public HostBean(){

    }

    public HostBean(String imei,String uri,long count){
        this.imei = imei;
        this.uri = uri;
        this.count = count;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(imei);
        out.writeUTF(uri);
        out.writeLong(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.imei = in.readUTF();
        this.uri = in.readUTF();
        this.count = in.readLong();

    }

    @Override
    public String toString() {
        return this.imei+"|"+this.count+"|"+this.uri;
    }

    public String getImei() {
        return imei;
    }

    public void setImei(String imei) {
        this.imei = imei;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }


}
