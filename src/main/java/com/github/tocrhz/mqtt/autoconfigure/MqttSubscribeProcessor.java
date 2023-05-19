package com.github.tocrhz.mqtt.autoconfigure;

import com.github.tocrhz.mqtt.annotation.MqttSubscribe;
import com.github.tocrhz.mqtt.subscriber.MqttSubscriber;
import com.github.tocrhz.mqtt.subscriber.SubscriberModel;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.LinkedList;

/**
 * When Bean is initialized, filter out the methods annotated with @MqttSubscribe, and create MqttSubscriber
 *
 * @author tocrhz
 * @see MqttSubscribe
 * @see MqttSubscriber
 */
@Component
public class MqttSubscribeProcessor implements BeanPostProcessor {

    // subscriber cache
    static final LinkedList<MqttSubscriber> SUBSCRIBERS = new LinkedList<>();

    @Value("${mqtt.disable:false}")
    private Boolean disable;

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        if (disable == null || !disable) {
            Method[] methods = bean.getClass().getMethods();
            for (Method method : methods) {
                if (method.isAnnotationPresent(MqttSubscribe.class)) {
                    //订阅配置
                    SubscriberModel model = SubscriberModel.of(method.getAnnotation(MqttSubscribe.class));
                    //订阅对象
                    SUBSCRIBERS.add(MqttSubscriber.of(model, bean, method));
                }
            }
        }
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        return bean;
    }
}
