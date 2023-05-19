package com.github.tocrhz.mqtt.subscriber;

import com.github.tocrhz.mqtt.autoconfigure.MqttConversionService;
import com.github.tocrhz.mqtt.exception.NullParameterException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.core.convert.converter.Converter;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.function.Function;

/**
 * Used to subscribe message
 *
 * @author tocrhz
 */
public class MqttSubscriber {
    private static final Logger log = LoggerFactory.getLogger(MqttSubscriber.class);

    /**
     * 消费消息
     *
     * @param clientId client id
     * @param topic message topic
     * @param mqttMessage receive message
     */
    public void accept(String clientId, String topic, MqttMessage mqttMessage) {
        //topic是否匹配
        Optional<TopicPair> matched = matched(clientId, topic);
        if (matched.isPresent()) {
            try {
                method.invoke(bean, fillParameters(matched.get(), topic, mqttMessage));
            } catch (NullParameterException ignored) {
                // 如果参数要求必填值，但真实为空，则不执行方法
                log.warn("Fill parameters caught null exception. client is {}, topic is {}", clientId, topic);
            } catch (IllegalAccessException | InvocationTargetException e) {
                log.error("Message handler error, client is {}, topic is {}", clientId, topic, e);
            }
        }
    }

    private SubscriberModel subscribe;
    private String[] clientIds;
    private Object bean;
    private Method method;
    /**
     * 订阅方法中所有参数信息
     */
    private LinkedList<ParameterModel> parameters;
    private int order;

    private final LinkedList<TopicPair> topics = new LinkedList<>();

    /**
     * 避免重复解析
     */
    private boolean hasResolveEmbeddedValue;

    public void afterInit(Function<String, String> function) {
        //是否已解析过
        if (hasResolveEmbeddedValue) {
            return;
        }
        hasResolveEmbeddedValue = true;
        if (function != null) {
            //替换clientId，eg：@MqttSubscribe(clients={"${mqtt.client-id:abc}", "test/${mqtt.client-id:abc}/+"})
            String[] clients = subscribe.clients();
            for (int i = 0; i < clients.length; i++) {
                clients[i] = function.apply(clients[i]);
            }
            //替换订阅topic，eg：@MqttSubscribe(value={"${mqtt.topics:xxx}"})
            String[] value = subscribe.value();
            for (int i = 0; i < value.length; i++) {
                value[i] = function.apply(value[i]);
            }
        }

        HashMap<String, Class<?>> paramTypeMap = new HashMap<>();
        this.parameters.stream()
                //@NamedValue("id") String id
                .filter(param -> param.getName() != null)
                .forEach(param -> paramTypeMap.put(param.getName(), param.getType()));
        this.clientIds = subscribe.clients();
        this.setTopics(subscribe, paramTypeMap);
    }

    public static MqttSubscriber of(SubscriberModel subscribe, Object bean, Method method) {
        MqttSubscriber subscriber = new MqttSubscriber();
        subscriber.bean = bean;
        subscriber.method = method;
        subscriber.subscribe = subscribe;
        //订阅方法设置的参数信息
        subscriber.parameters = ParameterModel.of(method);
        if (method.isAnnotationPresent(Order.class)) {
            Order order = method.getAnnotation(Order.class);
            subscriber.order = order.value();
        }
        return subscriber;
    }


    private void setTopics(SubscriberModel subscribe, HashMap<String, Class<?>> paramTypeMap) {
        String[] topics = subscribe.value();
        int[] qos = fillQos(topics, subscribe.qos());
        boolean[] shared = fillShared(topics, subscribe.shared());
        String[] groups = fillGroups(topics, subscribe.groups());
        LinkedHashSet<TopicPair> temps = new LinkedHashSet<>();
        for (int i = 0; i < topics.length; i++) {
            temps.add(TopicPair.of(topics[i], qos[i], shared[i], groups[i], paramTypeMap));
        }
        this.topics.addAll(temps);
        //topic优先级
        this.topics.sort(Comparator.comparingInt(TopicPair::order));
    }

    /**
     * 设置每一个topic对应的Qos信息
     *
     * @param topics topic collection
     * @param qos    topic Qos
     * @return Qos Array
     */
    private int[] fillQos(String[] topics, int[] qos) {
        int topic_len = topics.length;
        int qos_len = qos.length;
        //topic数量 > Qos数量：多余的topic服用最后一个topic的Qos
        if (topic_len > qos_len) {
            int[] temp = new int[topic_len];
            System.arraycopy(qos, 0, temp, 0, qos_len);
            Arrays.fill(temp, qos_len, topic_len, qos[qos_len - 1]);
            return temp;
        } else if (qos_len > topic_len) {
            int[] temp = new int[topic_len];
            System.arraycopy(qos, 0, temp, 0, topic_len);
            return temp;
        }
        return qos;
    }

    private boolean[] fillShared(String[] topics, boolean[] shared) {
        int topic_len = topics.length;
        int qos_len = shared.length;
        //topic数量 > Qos数量：多余的topic服用最后一个topic的shard配置
        if (topic_len > qos_len) {
            boolean[] temp = new boolean[topic_len];
            System.arraycopy(shared, 0, temp, 0, qos_len);
            Arrays.fill(temp, qos_len, topic_len, shared[qos_len - 1]);
            return temp;
        } else if (qos_len > topic_len) {
            boolean[] temp = new boolean[topic_len];
            System.arraycopy(shared, 0, temp, 0, topic_len);
            return temp;
        }
        return shared;
    }

    private String[] fillGroups(String[] topics, String[] groups) {
        int topic_len = topics.length;
        int qos_len = groups.length;
        //topic数量 > Qos数量：多余的topic服用最后一个topic的group配置
        if (topic_len > qos_len) {
            String[] temp = new String[topic_len];
            System.arraycopy(groups, 0, temp, 0, qos_len);
            Arrays.fill(temp, qos_len, topic_len, groups[qos_len - 1]);
            return temp;
        } else if (qos_len > topic_len) {
            String[] temp = new String[topic_len];
            System.arraycopy(groups, 0, temp, 0, topic_len);
            return temp;
        }
        return groups;
    }

    private Optional<TopicPair> matched(final String clientId, final String topic) {
        if (clientIds == null || clientIds.length == 0
                || Arrays.binarySearch(clientIds, clientId) >= 0) {
            return topics.stream()
                    .filter(pair -> pair.isMatched(topic))
                    .findFirst();
        }
        return Optional.empty();
    }

    /**
     * 填充方法体参数
     * @param topicPair topic message
     * @param topic topic
     * @param mqttMessage receive message
     * @return method parameter array
     */
    private Object[] fillParameters(TopicPair topicPair, String topic, MqttMessage mqttMessage) {
        //key-路径参数，value-真实值
        HashMap<String, String> pathValueMap = topicPair.getPathValueMap(topic);
        LinkedList<Object> objects = new LinkedList<>();
        //遍历订阅方法中所有参数信息，得到真实的值
        for (ParameterModel parameter : parameters) {
            //当前参数类型
            Class<?> target = parameter.getType();
            //路径参数变量名称
            String name = parameter.getName();
            //转换器
            LinkedList<Converter<Object, Object>> converters = parameter.getConverters();
            Object value = null;
            if (target == MqttMessage.class) {
                //直接使用MqttMessage接收消息
                value = mqttMessage;
            } else if (parameter.isSign() && mqttMessage != null) {
                //转换为具体的消息类型
                value = MqttConversionService.getSharedInstance().fromBytes(mqttMessage.getPayload(), target, converters);
            } else if (name != null) {
                //根据路径参数获取值
                if (pathValueMap.containsKey(name)) {
                    value = fromTopic(pathValueMap.get(name), target);
                }
            } else if (target == String.class) {
                //topic信息
                value = topic;
            } else if (target.getClassLoader() != null && mqttMessage != null) {
                //其他类型参数，如：包装类Integer、UserInfo对象
                value = MqttConversionService.getSharedInstance().fromBytes(mqttMessage.getPayload(), target, converters);
            }
            //校验参数值是否必传
            if (value == null) {
                if (parameter.isRequired()) {
                    throw new NullParameterException();
                }
                value = parameter.getDefaultValue();
            }
            objects.add(value);
        }
        return objects.toArray();
    }

    private Object fromTopic(String value, Class<?> target) {
        if (MqttConversionService.getSharedInstance()
                .canConvert(String.class, target)) {
            return MqttConversionService.getSharedInstance().convert(value, target);
        } else {
            log.warn("Unsupported covert from {} to {}", String.class.getName(), target.getName());
            return null;
        }
    }

    public int getOrder() {
        return order;
    }

    public LinkedList<TopicPair> getTopics() {
        return topics;
    }

    public boolean contains(String clientId) {
        if (this.clientIds == null || this.clientIds.length == 0) {
            return true; // for all client
        }
        for (String id : clientIds) {
            if (id.equals(clientId)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MqttSubscriber that = (MqttSubscriber) o;
        return Objects.equals(bean, that.bean) &&
                Objects.equals(method, that.method);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bean, method);
    }
}
