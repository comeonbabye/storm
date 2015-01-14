/**
 * (C) Copyright 2002-2007, Eucita Technologies Co., Ltd.<br>
 * All rights reserved.<br>
 * <br>
 * Created On : 2011-3-1 at 上午11:45:36<br>
 * User: EUCITA-wangtao <br>
 */

package com.storm.queue;

import javax.jms.Message;
import javax.jms.Session;

/**
 * Converter
 * @author EUCITA-wangtao
 *
 */
public interface MessageConverter<E> {

    Message createMsg(Session sesson, E e);

    E parse(Message msg);

}
