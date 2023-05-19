package com.github.tocrhz.mqtt.subscriber;

import com.github.tocrhz.mqtt.annotation.NamedValue;
import com.github.tocrhz.mqtt.annotation.Payload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.convert.converter.Converter;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.LinkedList;

/**
 * 订阅方法：每一个参数项对应的信息
 *
 * @author tocrhz
 */
final class ParameterModel {
    private static final Logger log = LoggerFactory.getLogger(ParameterModel.class);

    /**
     * 标记为消息内容，即当前参数是否配置 注解 @Payload
     * <br>
     * 若参数为String类型（即type=String.class）, 并且无标记(无注解 @Payload), 则赋值topic
     */
    private boolean sign;
    /**
     * 是否必填
     */
    private boolean required;
    /**
     * 参数类型
     */
    private Class<?> type;
    /**
     * 注解 @NamedValue 值，用于匹配路径参数定于，如：@MqttSubscribe(value="test/{id}") 参数 id
     */
    private String name;
    /**
     * 默认值
     */
    private Object defaultValue;
    /**
     * 自定义消息转换器
     */
    private LinkedList<Converter<Object, Object>> converters;

    private ParameterModel() {
    }

    public static LinkedList<ParameterModel> of(Method method) {
        LinkedList<ParameterModel> parameters = new LinkedList<>();
        //参数类型
        Class<?>[] parameterTypes = method.getParameterTypes();
        //注解
        Annotation[][] parameterAnnotations = method.getParameterAnnotations();

        for (int i = 0; i < parameterTypes.length; i++) {
            ParameterModel model = new ParameterModel();
            parameters.add(model);
            model.type = parameterTypes[i];
            model.defaultValue = defaultValue(model.type);
            Annotation[] annotations = parameterAnnotations[i];
            //是否存在注解
            if (annotations != null && annotations.length > 0) {
                for (Annotation annotation : annotations) {
                    //方法示例： public void sub(String topic, @NamedValue("id") String id)
                    if (annotation.annotationType() == NamedValue.class) {
                        NamedValue namedValue = (NamedValue) annotation;
                        model.required = model.required || namedValue.required();
                        model.name = namedValue.value();
                    }
                    // 方法示例：public void sub(String topic, MqttMessage message, @Payload String payload)
                    if (annotation.annotationType() == Payload.class) {
                        Payload payload = (Payload) annotation;
                        model.sign = true;
                        model.required = model.required || payload.required();
                        model.converters = toConverters(payload.value());
                    }
//                    if (annotation.annotationType() == NonNull.class) {
//                        model.required = true;
//                    }
                }
            }
        }
        return parameters;
    }

    @SuppressWarnings("unchecked")
    public static LinkedList<Converter<Object, Object>> toConverters(Class<? extends Converter<?, ?>>[] classes) {
        if (classes == null || classes.length == 0) {
            return null;
        } else {
            LinkedList<Converter<Object, Object>> converters = new LinkedList<>();
            for (Class<? extends Converter<?, ?>> covert : classes) {
                try {
                    converters.add((Converter<Object, Object>) covert.getDeclaredConstructor().newInstance());
                } catch (Exception e) {
                    log.error("Create converter instance failed.", e);
                }
            }
            return converters;
        }
    }

    public boolean isSign() {
        return sign;
    }

    public boolean isRequired() {
        return required;
    }

    public Class<?> getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public LinkedList<Converter<Object, Object>> getConverters() {
        return converters;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    /**
     * 默认值：<br>
     * 1. 基本数据类型：默认值 <br>
     * 2. 引用数据类型：null
     *
     * @param type class类型
     * @return 默认值
     */
    private static Object defaultValue(Class<?> type) {
        //基础数据类型：赋予默认值
        if (type.isPrimitive()) {
            if (type == boolean.class) {
                return false;
            }
            if (type == char.class) {
                return (char) 0;
            }
            if (type == byte.class) {
                return (byte) 0;
            }
            if (type == short.class) {
                return (short) 0;
            }
            if (type == int.class) {
                return 0;
            }
            if (type == long.class) {
                return 0L;
            }
            if (type == float.class) {
                return 0.0f;
            }
            if (type == double.class) {
                return 0.0d;
            }
        }
        return null;
    }
}
