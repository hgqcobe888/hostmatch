package com.spd.mr.zj.fillter;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class HostZjFillterBean implements Writable {
    /**imei*/
    private String imei;

    /**url*/
    private String uri;

    /**次数**/
    private long count;

    /**数据类型  0：当日新增数据   1：老数据result**/
    private String datatype;

    public HostZjFillterBean(){

    }

    public HostZjFillterBean(String imei, String uri, long count){
        this.imei = imei;
        this.uri = uri;
        this.count = count;
        this.datatype = datatype;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(imei);
        out.writeUTF(uri);
        out.writeLong(count);
        out.writeUTF(datatype);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.imei = in.readUTF();
        this.uri = in.readUTF();
        this.count = in.readLong();
        this.datatype = in.readUTF();

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

    public String getDatatype() {
        return datatype;
    }

    public void setDatatype(String datatype) {
        this.datatype = datatype;
    }



}
