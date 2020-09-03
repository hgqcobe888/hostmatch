package com.spd.mr.zj;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class HostBeanZj implements Writable {

    /**imei*/
    private String imei;

    /**设备号*/
    private String device;

    /**url*/
    private String uri;

    /**次数**/
    private long count;

    public HostBeanZj(){

    }

    public HostBeanZj(String imei,String device, String uri, long count){
        this.imei = imei;
        this.device = device;
        this.uri = uri;
        this.count = count;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(imei);
        out.writeUTF(device);
        out.writeUTF(uri);
        out.writeLong(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.imei = in.readUTF();
        this.device = in.readUTF();
        this.uri = in.readUTF();
        this.count = in.readLong();

    }

    @Override
    public String toString() {
        return this.imei+"|"+this.device+"|"+this.count+"|"+this.uri;
    }

    public String getImei() {
        return imei;
    }

    public void setImei(String imei) {
        this.imei = imei;
    }

    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
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
