package com.json.redis.connection;

import java.util.concurrent.TimeUnit;

import com.json.redis.command.*;

/**
 * Created by   on 12/21/16.
 */
public interface Connection extends StringCommand, ListCommand, HashCommand, SetCommand, SortSetCommand {

    boolean tryLock(String lock, int maxWait, int maxLockTimeAfterSuccess, TimeUnit timeUnit);

    void unlock();

    void forceUnlock();

    boolean isLocked();

    void close();

}
