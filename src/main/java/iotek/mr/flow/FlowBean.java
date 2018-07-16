package iotek.mr.flow;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements WritableComparable<FlowBean>{
    private String phone;
    private long upFlow;
    private long downFlow;
    private long countFlow;


    public int compareTo(FlowBean o) {
        if (this.countFlow>o.countFlow)
        {
            return -1;
        }
        return 1;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(phone);
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(countFlow);
    }

    public void readFields(DataInput dataInput) throws IOException {
        phone=dataInput.readUTF();
        upFlow=dataInput.readLong();
        downFlow=dataInput.readLong();
        countFlow=dataInput.readLong();
    }


    @Override
    public String toString() {
        return  phone  +
                "," + upFlow +
                "," + downFlow +
                "," + countFlow;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getCountFlow() {
        return countFlow;
    }

    public void setCountFlow(long countFlow) {
        this.countFlow = countFlow;
    }
}
