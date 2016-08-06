package com.cdcalabar.dmm.utils;

import com.cdcalabar.dmm.constants.DMMConstants;
import com.cdcalabar.dmm.exception.DMMIOException;
import com.cdcalabar.dmm.serializer.BaseSerializble;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by lkl on 16-8-3.
 */
public class DMMHiveUtils extends BaseSerializble{

    private static final Logger LOGGER = LoggerFactory.getLogger(DMMHiveUtils.class);

    private static final String PACKAGE_PREFIX = "java.lang.";

    /**
     * 确定指定字段的类型,不做复杂类型处理 array,map,bean
     *
     * @param type
     * @return
     */
    public static DataType getHiveFieldType(String type) {
        if (DataTypes.StringType.typeName().equals(type)) {
            return DataTypes.StringType;
        } else if (DataTypes.ShortType.typeName().equals(type)) {
            return DataTypes.ShortType;
        } else if (DataTypes.DoubleType.typeName().equals(type)) {
            return DataTypes.DoubleType;
        } else if (DataTypes.FloatType.typeName().equals(type)) {
            return DataTypes.FloatType;
        } else if (DataTypes.TimestampType.typeName().equals(type)) {
            return DataTypes.TimestampType;
        } else if (DataTypes.NullType.typeName().equals(type)) {
            return DataTypes.NullType;
        } else if (DataTypes.IntegerType.typeName().equals(type)) {
            return DataTypes.IntegerType;
        } else if (DataTypes.DateType.typeName().equals(type)) {
            return DataTypes.DateType;
        } else if (DataTypes.CalendarIntervalType.typeName().equals(type)) {
            return DataTypes.CalendarIntervalType;
        } else if (DataTypes.ByteType.typeName().equals(type)) {
            return DataTypes.ByteType;
        } else if (DataTypes.BooleanType.typeName().equals(type)) {
            return DataTypes.BooleanType;
        }

        return DataTypes.StringType;
    }

    /**
     * 构建schema
     *
     * @param colNames
     * @return
     */
    public static StructType buildStructType(String[] colNames) {
        List<StructField> structFields = new ArrayList<StructField>();
        for (int i = 0; i < colNames.length; i++) {
            String[] arr = colNames[i].split(DMMConstants.COLNUM_DELIMETER_COLON);
            structFields.add(DataTypes.createStructField(arr[0], getHiveFieldType(arr[1]), Integer.valueOf(arr[2]).equals(0) ? false : true));
        }
        StructType structType = DataTypes.createStructType(structFields);
        return structType;
    }


    /**
     * 通过反射获取元对象
     *
     * @param colNames
     * @return
     */
    public static Class[] getRelectClass(String[] colNames) throws DMMIOException {
        Class[] objs = new Class[colNames.length];
        for (int i = 0; i < colNames.length; i++) {
            String[] arr = colNames[i].split(DMMConstants.COLNUM_DELIMETER_COLON);
            String suffix = arr[1].toLowerCase().trim();
            suffix = StringUtils.capitalize(suffix);
            String class_pkg = PACKAGE_PREFIX + suffix;
            try {
                Class<?> cl = Class.forName(class_pkg);
                objs[i] = cl;
            } catch (Exception e) {
                LOGGER.error("未找到对应的类型："+class_pkg+e.getMessage());
                throw new DMMIOException("未找到对应的类型："+class_pkg+e.getMessage());
            }
        }
        return objs;
    }

    /**
     * 列值类型转换
     * @param colValues
     * @param colNames
     * @return
     */
    public static Object[] getObects(String[] colValues, String[] colNames) throws DMMIOException{
        Object[] objs = new Object[colValues.length];
        try {
            Class[] cls = DMMHiveUtils.getRelectClass(colNames);
            for (int i = 0; i < colValues.length; i++) {
                try {
                    Object obj = cls[i].getConstructor(String.class).newInstance(colValues[i]);
                    objs[i] = obj;
                }catch (Exception e){
                    LOGGER.error("类型强转错误："+colValues[i]+" to "+cls[i]+";\n"+e.getMessage());
                }
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            throw new DMMIOException(e.getMessage());
        }
        return objs;
    }

}
