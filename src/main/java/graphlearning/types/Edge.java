package graphlearning.types;

import lombok.Builder;
import lombok.Getter;

import java.util.List;

/** Edge. */
@Getter
@Builder
public class Edge {
    private Integer sourceNode;
    private Integer targetNode;
    private List<Byte> sourceEmbedding;
    private List<Byte> targetEmbedding;
    private Integer sourceLabel;
    private Integer targetLabel;
    private String timestamp;

    public String toString() {
        return "(" + sourceNode + ", " + targetNode + ", " + timestamp + ")";
    }
}
