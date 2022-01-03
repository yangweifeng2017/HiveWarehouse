package com.function.udaf;

import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

import java.util.HashMap;
import java.util.Map;

/**
 * @program: HiveWarehouse
 * @description: MyUDAF
 * @author: YangWeiFeng
 * @create: 2022/01/03 17:18
 */
/*
-- 建表
create table udaf_test(id int,name string,subject string,grade string);
-- 插入数据
insert into udaf_test values (1,'Mike','math','B'),(1,'Mike','science','C'),(1,'Mike','lanuage','B') ,(2,'Tom','math','B'),(2,'Tom','science','C'),(2,'Tom','sports','A');

init函数初始化
iterate接收传入的参数，进行迭代。返回类型为boolean。
terminatePartial无参数，类似于 hadoop的Combiner。
merge接收terminatePartial的返回结果，进行数据merge操作，其返回类型为boolean。
terminate返回最终的聚集函数结果。

select id,name,tempmerge(subject,grade) from udaf_test group by id,name;
 */
public class MyUDAF {
    public static class Evaluator implements UDAFEvaluator {
        private Map<String, String> map;

        //初始化
        public Evaluator() {
            init();
        }

        // 初始化函数间传递的中间变量
        public void init() {
            map = new HashMap<String, String>();
        }

        // map阶段，返回值为boolean类型，当为true则程序继续执行，当为false则程序退出
        public boolean iterate(String course, String score) {
            map.put(course, score);
            return true;
        }

        // 类似于combiner,局部聚合
        public Map<String, String> terminatePartial() {
            return map;
        }

        // reduce 阶段，用于逐个迭代处理map当中每个不同key对应的 terminatePartial的结果
        public boolean merge(Map<String, String> output) {
            this.map.putAll(output);
            return true;
        }

        // 处理merge计算完成后的结果，即对merge完成后的结果做最后的业务处理
        public String terminate() {
            return map.toString();
        }
    }
}
