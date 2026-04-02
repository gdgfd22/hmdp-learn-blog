package com.hmdp.lock;

public interface ILock {

    /**
     * 尝试获取锁
     * @param timeoutSec 锁持有时间
     * @return true=成功 false=失败
     */
    boolean tryLock(long timeoutSec);

    /**
     * 释放锁
     */
    void unlock();
}