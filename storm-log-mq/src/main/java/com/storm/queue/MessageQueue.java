package com.storm.queue;

public interface MessageQueue<E> {

    public E take() throws InterruptedException;
    
    public E take(long timeout) throws InterruptedException;
    
    public void push(E e);
    
    public void close();
}
