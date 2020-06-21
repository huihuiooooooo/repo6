package com.odh.entity;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author odh 2020/06/16
 * @JavaBean 自定义键类型
 */
public class MemberLogTime  implements WritableComparable<MemberLogTime> {
    private String member_name;
    private String logTime;
    @Override
    public int compareTo(MemberLogTime o){
        return this.getMember_name().compareTo(o.getMember_name());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(member_name);
        dataOutput.writeUTF(logTime);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.member_name = dataInput.readUTF();
        this.logTime = dataInput.readUTF();
    }

    public String getMember_name() {
        return member_name;
    }

    public void setMember_name(String member_name) {
        this.member_name = member_name;
    }

    public String getLogTime() {
        return logTime;
    }

    public void setLogTime(String logTime) {
        this.logTime = logTime;
    }

    @Override
    public String toString() {
        return this.member_name + "," + this.logTime;
    }
}
