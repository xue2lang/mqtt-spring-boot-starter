package com.github.tocrhz.mqtt.autoconfigure;

import com.github.tocrhz.mqtt.annotation.MqttSubscribe;
import com.github.tocrhz.mqtt.subscriber.MqttSubscriber;
import com.github.tocrhz.mqtt.subscriber.SubscriberModel;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;

/**
 * When Bean is initialized, filter out the methods annotated with @MqttSubscribe, and create MqttSubscriber
 *
 * @author tocrhz
 * @see MqttSubscribe
 * @see MqttSubscriber
 */
@Component
@ConditionalOnProperty(prefix = "mqtt", name = "disable", havingValue = "false", matchIfMissing = true)
public class MqttSubscribeProcessor implements BeanPostProcessor {

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        Method[] methods = bean.getClass().getMethods();
        for (Method method : methods) {
            if (method.isAnnotationPresent(MqttSubscribe.class)) {
                SubscriberModel model = SubscriberModel.of(method.getAnnotation(MqttSubscribe.class));
                MqttSubscriber.SUBSCRIBERS.add(MqttSubscriber.of(model, bean, method));
            }
        }
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        return bean;
    }
}
