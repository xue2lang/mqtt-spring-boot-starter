package com.github.tocrhz.mqtt.subscriber;

/**
 * @author tjheiska
 */
class TopicParam {
    /**
     * 参数名称， eg：topic={projectId}/{userName}，则name=projectId、name=userName
     */
    private String name;
    private int at; // 正则匹配的参数位置.

    public TopicParam(String name, int at) {
        super();
        this.name = name;
        this.at = at;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAt() {
        return at;
    }

    public void setAt(int at) {
        this.at = at;
    }
}
