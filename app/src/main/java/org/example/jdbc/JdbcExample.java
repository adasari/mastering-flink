package org.example.jdbc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.source.JdbcSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class JdbcExample {

    public static class Product {
        private long id;
        private String name;
        private String address;

        public Product(long id, String name, String address) {
            this.id = id;
            this.name = name;
            this.address = address;
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        JdbcSource<Product> jdbcSource = JdbcSource.<Product>builder()
                .setSql("SELECT id, name, address FROM products")
                .setDriverName("org.postgresql.Driver")
                .setDBUrl("jdbc:postgresql://localhost:5432/test")
                .setUsername("postgres")
                .setPassword("postgres")
                .setTypeInformation(TypeInformation.of(Product.class))
                .setResultExtractor((resultSet) ->
                    new Product(resultSet.getLong("id"), resultSet.getString("name"), resultSet.getString("address"))
                )
                .build();

        // Create a Flink DataStream using the JdbcSource
        DataStream<Product> jdbcDataStream = env.fromSource(
                jdbcSource,
                WatermarkStrategy.noWatermarks(),
                "JdbcSource"
        );

        // Print the data to the console
        jdbcDataStream.print();

        // Execute the Flink job
        env.execute("JDBC Source Example");
    }
}
