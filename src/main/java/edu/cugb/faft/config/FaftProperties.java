package edu.cugb.faft.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "faft")
public class FaftProperties {

    private double alpha;
    private double beta;
    private double gamma;
    private double rmin; // 最小采样率
    private double rmax; // 最大采样率
    private double step;
    @Value("${faft.error-threshold}")
    private double errorThreshold; // 错误阈值

    @Value("${faft.decay-alpha}")
    private double decayAlpha; // 衰减系数

    @Value("${faft.impact-delta}")
    private double impactDelta;

    // getters & setters
    public double getAlpha() { return alpha; }
    public void setAlpha(double alpha) { this.alpha = alpha; }
    public double getBeta() { return beta; }
    public void setBeta(double beta) { this.beta = beta; }
    public double getGamma() { return gamma; }
    public void setGamma(double gamma) { this.gamma = gamma; }
    public double getRmin() { return rmin; }
    public void setRmin(double rmin) { this.rmin = rmin; }
    public double getRmax() { return rmax; }
    public void setRmax(double rmax) { this.rmax = rmax; }
    public double getStep() { return step; }
    public void setStep(double step) { this.step = step; }
    public double getErrorThreshold() { return errorThreshold; }
    public void setErrorThreshold(double errorThreshold) { this.errorThreshold = errorThreshold; }
    public double getDecayAlpha() { return decayAlpha; }
    public void setDecayAlpha(double decayAlpha) { this.decayAlpha = decayAlpha; }
    public double getImpactDelta() { return impactDelta; }
    public void setImpactDelta(double impactDelta) { this.impactDelta = impactDelta; }
}
