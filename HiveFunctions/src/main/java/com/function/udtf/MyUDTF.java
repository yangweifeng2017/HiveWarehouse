package com.function.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @program: HiveWarehouse
 * @description: MyUDTF
 * @author: YangWeiFeng
 * @create: 2022/01/03 17:11
 */

/*
 创建了一张表，包含id、name、score三列。
 其他列随意，name列中，包含的数据需要存在分隔符数据的，比如‘Mike,Tom,John,Allen’。
 测试函数
 select split_udtf("aa,bb,cc",',');
 进行表测试
 select split_udtf(name,',') from udtf_test;
 单独查询列没有问题，若这时需要进行多列的查询，则会报错：UDTF’s are not supported outside the SELECT clause,
 nor nested in expressions。这样直接查询是UDTF函数所不执行的，所以需要使用lateral view。

 ## 使用lateral view
 select id,names,score from udtf_test lateral view split_udtf(name,',') temp as names;
 ## 事实上，如果需要达到行转列的效果，Hive本身已经提供很好的支持
 select id,names,score from udtf_test lateral view explode(split(name,',')) temp as names;
 */


public class MyUDTF extends GenericUDTF {
    private List<String> list = new ArrayList<String>();

    //定义输出数据的列名和数据类型
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        //输出数据的列名
        ArrayList<String> fieldNames = new ArrayList<String>();
        //列名取名
        fieldNames.add("word");

        //输出数据的类型
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        //根据将来的返回值确定返回的类型
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        //返回定义的数据
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }


    //对数据进行操作
    @Override
    public void process(Object[] args) throws HiveException {
        //获取数据
        String data = args[0].toString();
        //获取分隔符
        String splitKey = args[1].toString();
        //切分数据
        String[] words = data.split(splitKey);
        //遍历输出
        for (String word : words) {
            //将数据放入集合
            list.clear();
            list.add(word);
            //使用自带方法写出
            forward(list);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
