package com.cdcalabar.dmm.io;

import com.cdcalabar.dmm.serializer.BaseSerializble;
import jline.Terminal;

/**
 * Created by lkl on 16-8-4.
 * 查询，建表的字段说明
 */
public class DMMEntry extends BaseSerializble{

    /** 字段名 */
    private String fieldName;

    /** 字段值 */
    private Object fieldVale;

    /** 是否目标字段 */
    private boolean labelFiled;

    /** 是否特征字段 */
    private boolean featureFiled;

    /** 字段类型 */
    private String fieldType;

    /** 字段是否可以为空 */
    private boolean nullAble;

    public boolean isLabelFiled() {
        return labelFiled;
    }

    public void setLabelFiled(boolean labelFiled) {
        this.labelFiled = labelFiled;
    }

    public boolean isFeatureFiled() {
        return featureFiled;
    }

    public void setFeatureFiled(boolean featureFiled) {
        this.featureFiled = featureFiled;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public Object getFieldVale() {
        return fieldVale;
    }

    public void setFieldVale(Object fieldVale) {
        this.fieldVale = fieldVale;
    }

    public String getFieldType() {
        return fieldType;
    }

    public void setFieldType(String fieldType) {
        this.fieldType = fieldType;
    }

    public boolean isNullAble() {
        return nullAble;
    }

    public void setNullAble(boolean nullAble) {
        this.nullAble = nullAble;
    }


}
