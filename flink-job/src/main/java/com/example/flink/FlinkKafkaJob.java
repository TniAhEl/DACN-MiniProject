package com.example.flink;

import com.example.flink.avro.Customer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

public class FlinkKafkaJob {

        public static void main(String[] args) throws Exception {

                final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.enableCheckpointing(5000);

                final String KAFKA_BOOTSTRAP_SERVERS = "kafka:29092";
                final String SCHEMA_REGISTRY_URL = "http://schema-registry:8081";
                final String INPUT_TOPIC = "mysql_server.final.customer";
                final String CONSUMER_GROUP_ID = "flink-group";

                final String POSTGRES_URL = "jdbc:postgresql://host.docker.internal:5433/postgres";
                final String POSTGRES_USER = "postgres";
                final String POSTGRES_PASSWORD = "123456";
                final String POSTGRES_TABLE = "flink_output_customer";

                // 🔹 Kafka Source
                KafkaSource<Customer> source = KafkaSource.<Customer>builder()
                                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                                .setTopics(INPUT_TOPIC)
                                .setGroupId(CONSUMER_GROUP_ID)
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setValueOnlyDeserializer(
                                                ConfluentRegistryAvroDeserializationSchema.forSpecific(Customer.class,
                                                                SCHEMA_REGISTRY_URL))
                                .build();

                DataStream<Customer> customerStream = env.fromSource(
                                source,
                                WatermarkStrategy.noWatermarks(),
                                "Kafka Customer Source");

                // 🔹 Log dữ liệu
                customerStream.map((MapFunction<Customer, Customer>) c -> {
                        System.out.println("📥 Received from Kafka: " + c);
                        return c;
                });

                // 🔹 Xử lý dữ liệu
                DataStream<Customer> processedStream = customerStream
                                .map((MapFunction<Customer, Customer>) customer -> {
                                        if (customer.getFname() != null)
                                                customer.setFname(customer.getFname().toString().toUpperCase());
                                        if (customer.getLname() != null)
                                                customer.setLname(customer.getLname().toString().toUpperCase());
                                        return customer;
                                });

                // 🔹 Ghi vào PostgreSQL
                processedStream.addSink(
                                JdbcSink.sink(
                                                "INSERT INTO " + POSTGRES_TABLE + " (" +
                                                                "id, username, password, fname, lname, bday, address, email, phone, "
                                                                +
                                                                "create_at, update_at, status" +
                                                                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                                                                "ON CONFLICT (id) DO UPDATE SET " +
                                                                "username = EXCLUDED.username, " +
                                                                "password = EXCLUDED.password, " +
                                                                "fname = EXCLUDED.fname, " +
                                                                "lname = EXCLUDED.lname, " +
                                                                "bday = EXCLUDED.bday, " +
                                                                "address = EXCLUDED.address, " +
                                                                "email = EXCLUDED.email, " +
                                                                "phone = EXCLUDED.phone, " +
                                                                "create_at = EXCLUDED.create_at, " +
                                                                "update_at = EXCLUDED.update_at, " +
                                                                "status = EXCLUDED.status",
                                                (ps, c) -> {
                                                        ps.setLong(1, c.getId());
                                                        ps.setString(2, toStr(c.getUsername()));
                                                        ps.setString(3, toStr(c.getPassword()));
                                                        ps.setString(4, toStr(c.getFname()));
                                                        ps.setString(5, toStr(c.getLname()));

                                                        LocalDate bday = c.getBday();
                                                        if (bday != null) {
                                                                ps.setDate(6, Date.valueOf(bday)); // chuyển LocalDate →
                                                                                                   // java.sql.Date
                                                        } else {
                                                                ps.setNull(6, java.sql.Types.DATE);
                                                        }

                                                        ps.setString(7, toStr(c.getAddress()));
                                                        ps.setString(8, toStr(c.getEmail()));
                                                        ps.setString(9, toStr(c.getPhone()));

                                                        // ✅ create_at: kiểu TIMESTAMP
                                                        setTimestampSafely(ps, 10, c.getCreateAt());
                                                        // ✅ update_at: kiểu TIMESTAMP
                                                        setTimestampSafely(ps, 11, c.getUpdateAt());

                                                        ps.setString(12, toStr(c.getStatus()));
                                                },
                                                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                                                .withUrl(POSTGRES_URL)
                                                                .withDriverName("org.postgresql.Driver")
                                                                .withUsername(POSTGRES_USER)
                                                                .withPassword(POSTGRES_PASSWORD)
                                                                .build()));

                System.out.println("🚀 Flink job started, waiting for Kafka messages...");
                env.execute("Flink CDC Avro → PostgreSQL");
        }

        private static String toStr(Object o) {
                return o == null ? null : o.toString();
        }

        private static void setTimestampSafely(java.sql.PreparedStatement ps, int index, Object field)
                        throws java.sql.SQLException {
                if (field == null) {
                        ps.setNull(index, java.sql.Types.TIMESTAMP);
                        return;
                }

                if (field instanceof Long) {
                        // Nếu là epoch milli
                        ps.setTimestamp(index, new Timestamp((Long) field));
                } else if (field instanceof CharSequence) {
                        // Nếu là chuỗi kiểu "2025-07-30 11:44:26.422702+00"
                        try {
                                Instant instant = Instant.parse(field.toString().replace(" ", "T").replace("+00", "Z"));
                                ps.setTimestamp(index, Timestamp.from(instant));
                        } catch (Exception e) {
                                ps.setNull(index, java.sql.Types.TIMESTAMP);
                        }
                } else if (field instanceof Instant) {
                        ps.setTimestamp(index, Timestamp.from((Instant) field));
                } else {
                        ps.setNull(index, java.sql.Types.TIMESTAMP);
                }
        }
}
