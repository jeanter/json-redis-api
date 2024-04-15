package com.test;


import java.io.Serializable;

/**
 * Created by lvhaizhen on 2018/1/17.
 */
public class CacheObject<T> implements Serializable{
    private T object;
    private long time;
    public CacheObject(T obj, long time){
        this.object = obj;
        this.time = time;
    }

    public void setValueAndTime(T obj, long time){
        this.object = obj;
        this.time = time;
    }

    public long getTime(){
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public T getObject(){
        return object;
    }

    public boolean isValid(long expireTime){
        long currentTime = System.currentTimeMillis();
        return currentTime - getTime() < expireTime;
    }

}