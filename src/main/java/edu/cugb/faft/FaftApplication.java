package edu.cugb.faft;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 仅作为 Spring 容器入口，供 Launcher 获取配置与 Bean。
 * 提交 Storm 集群时，仍然使用 Launcher 的 main 作为入口。
 */
@SpringBootApplication
public class FaftApplication {
    public static void main(String[] args) {
        SpringApplication.run(FaftApplication.class, args).close();
    }
}
