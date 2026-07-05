package Application;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = {"Application", "Controller"})  // 加上这行，手动扫描两个包
public class MovieApplication {
    public static void main(String[] args) {
        SpringApplication.run(MovieApplication.class, args);
        System.out.println("🎬 系统启动成功！");
        System.out.println("📍 实时大屏: http://localhost:8080");
        System.out.println("📍 推荐系统:http://localhost:8080/recommend.html");
    }
}