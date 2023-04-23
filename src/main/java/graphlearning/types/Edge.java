package graphlearning.types;

import lombok.Builder;
import lombok.Getter;

/** Edge. */
@Getter
@Builder
public class Edge {
    private Integer sourceNode;
    private Integer targetNode;
    private byte[] sourceEmbedding;
    private byte[] targetEmbedding;
    private Integer sourceLabel;
    private Integer targetLabel;
    private String timestamp;

    public String toString() {
        return "(" + sourceNode + ", " + targetNode + ", " + timestamp + ")";
    }
}
