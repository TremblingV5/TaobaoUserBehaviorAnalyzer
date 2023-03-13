package org.personal.xinzf.agg;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.personal.xinzf.pojos.UserBehaviour;

public class CountAgg implements AggregateFunction<UserBehaviour, Integer, Integer> {

    @Override
    public Integer createAccumulator() {
        return 0;
    }

    @Override
    public Integer add(UserBehaviour userBehaviour, Integer integer) {
        return integer + 1;
    }

    @Override
    public Integer getResult(Integer integer) {
        return integer;
    }

    @Override
    public Integer merge(Integer integer, Integer acc1) {
        return integer + acc1;
    }
}
