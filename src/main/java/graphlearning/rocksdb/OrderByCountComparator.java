package graphlearning.rocksdb;

import org.rocksdb.AbstractComparator;
import org.rocksdb.ComparatorOptions;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/** RocksDB Comparator. Sort by id then by count. */
public class OrderByCountComparator extends AbstractComparator {

    public OrderByCountComparator(ComparatorOptions options) {
        super(options);
    }

    @Override
    public String name() {
        return "OrderByCount";
    }

    @Override
    public int compare(ByteBuffer a, ByteBuffer b) {
        Charset charset = Charset.forName("UTF-8");
        String aStr = charset.decode(a).toString();
        String bStr = charset.decode(b).toString();
        String[] aStrs = aStr.split("\\|");
        String[] bStrs = bStr.split("\\|");
        int aId = Integer.parseInt(aStrs[0]);
        int bId = Integer.parseInt(bStrs[0]);
        int aCount = Integer.parseInt(aStrs[1]);
        int bCount = Integer.parseInt(bStrs[1]);
        if (aId == bId) {
            return aCount - bCount;
        } else {
            return aId - bId;
        }
    }
}
