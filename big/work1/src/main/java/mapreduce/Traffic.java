package mapreduce;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author Jonas Gao
 * @since 2022/3/12
 */
public class Traffic extends BinaryComparable implements WritableComparable<BinaryComparable> {

    private int upstream;

    private int downstream;

    public Traffic() {
    }

    public Traffic(int upstream, int downstream) {
        this.upstream = upstream;
        this.downstream = downstream;
    }

    public int getUpstream() {
        return upstream;
    }

    public void setUpstream(int upstream) {
        this.upstream = upstream;
    }

    public int getDownstream() {
        return downstream;
    }

    public void setDownstream(int downstream) {
        this.downstream = downstream;
    }

    @Override
    public int getLength() {
        return 8;
    }

    @Override
    public byte[] getBytes() {
        ByteBuffer allocate = ByteBuffer.allocate(8);
        allocate.putInt(upstream);
        allocate.putInt(downstream);
        return allocate.array();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(upstream);
        out.writeInt(downstream);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        upstream = in.readInt();
        downstream = in.readInt();
    }

    @Override
    public String toString() {
        return upstream + "\t" + downstream;
    }
}