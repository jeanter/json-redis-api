package com.json.redis;

/**
 * cluster 对于mget,mset的实现方式
 * 0:循环
 * 1:通过pipeline
 *
 * @author  
 */
public enum BatchModel {

    ITERATION(0), PIPELINE(1);

    private int value;

    BatchModel(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public static BatchModel getModelByValue(int value) {
        BatchModel resultEnum = ITERATION;
        BatchModel[] enumAry = BatchModel.values();
        for (int i = 0; i < enumAry.length; i++) {
            if (enumAry[i].getValue() == value) {
                resultEnum = enumAry[i];
                break;
            }
        }
        return resultEnum;
    }
}
