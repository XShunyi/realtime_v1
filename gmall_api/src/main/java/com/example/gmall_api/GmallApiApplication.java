package com.example.gmall_api;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan("com.example.gmall_api.mapper")
public class GmallApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(GmallApiApplication.class, args);
    }

}
