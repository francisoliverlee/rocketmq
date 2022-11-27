package org.apache.rocketmq.common.utils;

import org.junit.Assert;
import org.junit.Test;

public class DataConverterTest {
    @Test
    public void testCheckExistFlagIsTrue() {
        int bits = 0;
        // 设置0 存在于bit
        int newBits = DataConverter.setBit(bits, 0, true);
        System.out.println(newBits);

        // 判断0 是否存在
        Assert.assertTrue(DataConverter.getBit(newBits, 0));
    }

    @Test
    public void testCheckExistFalse() {
        int bits = 0;
        // 设置0 存在于bit

        int newBits = DataConverter.setBit(bits, 1, false);
        System.out.println(newBits);

        // 判断0 是否存在
        Assert.assertFalse(DataConverter.getBit(newBits, 0));
        Assert.assertTrue(DataConverter.getBit(newBits, 1));
    }
}
