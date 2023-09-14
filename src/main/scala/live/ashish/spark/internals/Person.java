package live.ashish.spark.internals;

import lombok.*;

import java.io.Serializable;

@Data @AllArgsConstructor
public class Person implements Serializable {
    private String name;
    private int age;
}
