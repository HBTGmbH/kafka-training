package de.hbt.edu.kafka.examplestreamapp;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.Consumer;
import java.util.function.Function;

@Configuration
public class Functions {

    @Bean
    public Function<KStream<String, String>, KStream<String, String>> process() {
        return input -> input.map((key, value) -> {
            System.out.println(key + ", " + value);
            return new KeyValue<>(key, "new-value");
        });
    }

    @Bean
    public Consumer<KStream<String, String>> salesData() {
        return message -> {
            message.foreach((key, value) -> {
                System.out.println(key + " -> " + value);
            });
        };
    }

}
