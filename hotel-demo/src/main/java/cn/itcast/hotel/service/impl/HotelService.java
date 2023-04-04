package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {

    @Autowired
    private RestHighLevelClient client;

    @Override
    public PageResult search(RequestParams params){
        try {
            //1. 准备request
            SearchRequest request = new SearchRequest("hotel");
            //2. 准备DSL
            //2.1 query
            // 构建BooleanQuery
            buildBasicQuery(params,request);

            //2.2 分页
            int page = params.getPage();
            int size = params.getSize();
            request.source().from( (page -1) * size ).size(size);
            //3. 发送请求，得到响应
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            //4. 解析响应
            return handleResponse(response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void buildBasicQuery(RequestParams params, SearchRequest request) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        // 关键字搜索
        String key = params.getKey();
        String city = params.getCity();
        String brand = params.getBrand();
        String starName = params.getStarName();
        Integer minPrice = params.getMinPrice();
        Integer maxPrice = params.getMaxPrice();
        if (key == null || "".equals(key)){
            boolQuery.must(QueryBuilders.matchAllQuery());
        }else {
            boolQuery.must(
                    QueryBuilders.matchQuery("all",key));
        }
        // 城市条件
        if(city != null && !"".equals(city)){
            boolQuery.filter(QueryBuilders.termQuery("city",city));
        }
        // 品牌条件
        if(brand != null && !"".equals(brand)){
            boolQuery.filter(QueryBuilders.termQuery("brand",brand));
        }
        // 星级条件
        if(starName != null && !"".equals(starName)){
            boolQuery.filter(QueryBuilders.termQuery("starName",starName));
        }
        // 价格
        if (maxPrice != null && minPrice != null){
            boolQuery.filter(QueryBuilders.rangeQuery("price").gte(minPrice).lte(maxPrice));
        }
        request.source().query(boolQuery);
    }

    private PageResult handleResponse(SearchResponse response) {
        //4. 解析响应
        SearchHits searchHits = response.getHits();
        //4.1. 获取总条数
        long total = searchHits.getTotalHits().value;
        //4.2. 文档数组
        SearchHit[] hits = searchHits.getHits();
        //4.3. 遍历
        List<HotelDoc> hotels = new ArrayList<>();
        for (SearchHit hit: hits) {
            //获取文档source
            String json = hit.getSourceAsString();
            // 反序列化
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            hotels.add(hotelDoc);
        }
        //4.4 封装返回
        return new PageResult(total,hotels);
    }
}
