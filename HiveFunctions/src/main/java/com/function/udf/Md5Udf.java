package com.function.udf;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * ClassName Md5Udf
 * 功能: MD5 udf函数
 * Author YangWeiFeng
 * Date 2019/11/27 10:34
 * Version 1.0
 * 部署方式:
 * 永久生效
 * 将jar包上传到 /user_ext/ad_engine/udf_warehouse
 * CREATE FUNCTION ad_angine_md5 AS 'com.adengine.udf.Md5Udf' using jar 'viewfs://c9//user_ext/ad_engine/udf_warehouse/ad_engine_udf_warehouse-1.0-jar-with-dependencies.jar';
 * 如需更改，只需要更改相应的jar包，重新上传即可
 **/
public class Md5Udf extends UDF {
    /**
     * 需要Java代码继承UDF，但是编译器不会要求必须实现某个函数。需要开发者自己实现evaluate函数，该函数可以重载多个，设置不同的参数等。
     *
     * @param args 传递参数值
     * @param type Boolean true为大写 false为小写
     * @return md5
     */
    public String evaluate(String args, Boolean type) {
        String md5 = DigestUtils.md5Hex(args);
        if (type) {
            return md5.toUpperCase();
        } else {
            return md5;
        }
    }

    public String evaluate(String args) {
        return DigestUtils.md5Hex(args);
    }
}