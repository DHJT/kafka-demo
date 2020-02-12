package tech.dhjt.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import springfox.documentation.swagger2.annotations.EnableSwagger2;

@EnableAspectJAutoProxy
@SpringBootApplication(scanBasePackages = { "tech.dhjt.kafka" })
@EnableSwagger2
@EnableAsync
@EnableTransactionManagement
public class AppApplication {

    public static void main(String[] args) throws Exception {
        SpringApplication.run(AppApplication.class, args);
    }

}
