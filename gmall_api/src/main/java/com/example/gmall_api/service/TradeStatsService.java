package com.example.gmall_api.service;



import com.example.gmall_api.bean.TradeProvinceOrderAmount;
import com.example.gmall_api.bean.TradeProvinceOrderCt;
import com.example.gmall_api.bean.TradeStats;

import java.util.List;

public interface TradeStatsService {
    Double getTotalAmount(Integer date);

    List<TradeStats> getTradeStats(Integer date);

    List<TradeProvinceOrderCt> getTradeProvinceOrderCt(Integer date);

    List<TradeProvinceOrderAmount> getTradeProvinceOrderAmount(Integer date);
}