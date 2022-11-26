/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store.pop;

import com.alibaba.fastjson.annotation.JSONField;
import java.util.ArrayList;
import java.util.List;

public class PopCheckPoint {
    // 本次pop拉取消息的起始 message queue offset
    @JSONField(name = "so")
    private long startOffset;
    // pop发生的时间，单位毫秒
    @JSONField(name = "pt")
    private long popTime;
    // 这批消息的不可见时间
    @JSONField(name = "it")
    private long invisibleTime;
    // 当前这批消息的 ack情况， 通过二进制运算，记录了每个消息是否被ack了
    @JSONField(name = "bm")
    private int bitMap;
    // 本次pop拉取到的消息条数
    @JSONField(name = "n")
    private byte num;
    // 本次pop拉取的queue id
    @JSONField(name = "q")
    private byte queueId;
    // topic
    @JSONField(name = "t")
    private String topic;
    // 消费者组id
    @JSONField(name = "c")
    private String cid;
    // 恢复 位点？？？
    @JSONField(name = "ro")
    private long reviveOffset;
    // 消息位点差 xxx - yyy ???
    //
    @JSONField(name = "d")
    private List<Integer> queueOffsetDiff;
    @JSONField(name = "bn")
    String brokerName;

    public long getReviveOffset() {
        return reviveOffset;
    }

    public void setReviveOffset(long reviveOffset) {
        this.reviveOffset = reviveOffset;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(long startOffset) {
        this.startOffset = startOffset;
    }

    public void getStartOffset(long startOffset) {
        this.startOffset = startOffset;
    }

    public void setPopTime(long popTime) {
        this.popTime = popTime;
    }

    public void setInvisibleTime(long invisibleTime) {
        this.invisibleTime = invisibleTime;
    }

    public long getPopTime() {
        return popTime;
    }

    public long getInvisibleTime() {
        return invisibleTime;
    }

    public long getReviveTime() {
        return popTime + invisibleTime;
    }

    public int getBitMap() {
        return bitMap;
    }

    public void setBitMap(int bitMap) {
        this.bitMap = bitMap;
    }

    public byte getNum() {
        return num;
    }

    public void setNum(byte num) {
        this.num = num;
    }

    public byte getQueueId() {
        return queueId;
    }

    public void setQueueId(byte queueId) {
        this.queueId = queueId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getCId() {
        return cid;
    }

    public void setCId(String cid) {
        this.cid = cid;
    }

    public List<Integer> getQueueOffsetDiff() {
        return queueOffsetDiff;
    }

    public void setQueueOffsetDiff(List<Integer> queueOffsetDiff) {
        this.queueOffsetDiff = queueOffsetDiff;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public void addDiff(int diff) {
        if (this.queueOffsetDiff == null) {
            this.queueOffsetDiff = new ArrayList<>(8);
        }
        this.queueOffsetDiff.add(diff);
    }

    public int indexOfAck(long ackOffset) {
        if (ackOffset < startOffset) {
            return -1;
        }

        // old version of checkpoint
        if (queueOffsetDiff == null || queueOffsetDiff.isEmpty()) {

            if (ackOffset - startOffset < num) {
                return (int) (ackOffset - startOffset);
            }

            return -1;
        }

        // new version of checkpoint
        return queueOffsetDiff.indexOf((int) (ackOffset - startOffset));
    }

    public long ackOffsetByIndex(byte index) {
        // old version of checkpoint
        if (queueOffsetDiff == null || queueOffsetDiff.isEmpty()) {
            return startOffset + index;
        }

        return startOffset + queueOffsetDiff.get(index);
    }

    @Override
    public String toString() {
        return "PopCheckPoint [topic=" + topic + ", cid=" + cid + ", queueId=" + queueId + ", startOffset=" + startOffset + ", bitMap=" + bitMap + ", num=" + num + ", reviveTime=" + getReviveTime()
                + ", reviveOffset=" + reviveOffset + ", diff=" + queueOffsetDiff + ", brokerName=" + brokerName + "]";
    }

}
