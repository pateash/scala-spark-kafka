package live.ashish.spark.internals;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data @AllArgsConstructor @NoArgsConstructor
public class People implements Serializable {
    private String name;
    private int age;
    private String classification;
}
