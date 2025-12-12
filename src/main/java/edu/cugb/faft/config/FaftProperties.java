package edu.cugb.faft.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "faft")
@Data
public class FaftProperties {

    private double alpha;
    private double beta;
    private double gamma;
    private double rmin; // 最小采样率
    private double rmax; // 最大采样率
    private double step;
    private double errorThreshold; // 错误阈值
    private double decayAlpha; // 衰减系数
    private double impactDelta;

}
