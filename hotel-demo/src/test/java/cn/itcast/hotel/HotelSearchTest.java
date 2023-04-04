package cn.itcast.hotel;

import cn.itcast.hotel.pojo.HotelDoc;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

@SpringBootTest
public class HotelSearchTest {

    @Autowired
    private RestHighLevelClient client;

    @Test
    void testMatchAll() throws IOException {
        //1.准备Request对象
        SearchRequest request = new SearchRequest("hotel");
        //2。准备DSL
        request.source().query(QueryBuilders.matchAllQuery());
        //3. 发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //4. 解析响应
        handleResponse(response);
    }

    @Test
    void testMatch() throws IOException {
        //1.准备Request对象
        SearchRequest request = new SearchRequest("hotel");
        //2。准备DSL
        //request.source().query(QueryBuilders.matchQuery("all","如家"));
        request.source().query(QueryBuilders.multiMatchQuery("如家","name","business"));
        //3. 发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //4. 解析响应
        handleResponse(response);
    }

    @Test
    void testTerm() throws IOException {
        //1.准备Request对象
        SearchRequest request = new SearchRequest("hotel");
        //2。准备DSL
        //request.source().query(QueryBuilders.termQuery("city","上海"));
        request.source().query(QueryBuilders.rangeQuery("price").gte(100).lte(150));
        //3. 发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //4. 解析响应
        handleResponse(response);
    }

    @Test
    void testBool() throws IOException {
        //1.准备Request对象
        SearchRequest request = new SearchRequest("hotel");
        //2。准备DSL
        //2.1 准备BooleanQuery
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        //2.2 添加term
        //2.3 添加filter
        boolQuery.must(QueryBuilders.termQuery("city","上海"))
                .filter(QueryBuilders.rangeQuery("price").gte(200).lte(250));
        request.source().query(boolQuery);
        // 利用链式编程
        /*request.source()
                .query(QueryBuilders.boolQuery()
                    .must(QueryBuilders.termQuery("city", "上海"))
                    .filter(QueryBuilders.rangeQuery("price").gte(200).lte(250)));*/
        //3. 发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //4. 解析响应
        handleResponse(response);
    }

    @Test
    void testPageAndSort() throws IOException {
        // 页码，每页大小
        int page = 2,size = 5;
        //1.准备Request对象
        SearchRequest request = new SearchRequest("hotel");
        //2。准备DSL
//        //2.1 query
//        request.source().query(QueryBuilders.matchAllQuery());
//        //2.2 排序sort
//        request.source().sort("price", SortOrder.ASC);
//        //2.3 分页from size
//        request.source().from( (page - 1) * size).size(size);
        // 链式编程
        request.source()
                .query(QueryBuilders.matchAllQuery())
                .sort("price",SortOrder.ASC)
                .from((page - 1) * size).size(size);
        //3. 发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //4. 解析响应
        handleResponse(response);
    }

    @Test
    void testHignlight() throws IOException {
        //1.准备Request对象
        SearchRequest request = new SearchRequest("hotel");
        //2。准备DSL
        //2.1 query
        request.source().query(QueryBuilders.matchQuery("all","如家"));
        //2.2 highlight
        request.source().highlighter(new HighlightBuilder().field("name").requireFieldMatch(false));
        //3. 发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //4. 解析响应
        handleResponse(response);
    }



    private void handleResponse(SearchResponse response) {
        //4. 解析响应
        SearchHits searchHits = response.getHits();
        //4.1. 获取总条数
        long total = searchHits.getTotalHits().value;
        System.out.println("共搜索到" + total + "条数据");
        //4.2. 文档数组
        SearchHit[] hits = searchHits.getHits();
        //4.3. 遍历
        for (SearchHit hit: hits) {
            //获取文档source
            String json = hit.getSourceAsString();
            // 反序列化
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            //获取高亮字段结果
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            if (CollectionUtils.isEmpty(highlightFields)){
                // 根据字段名获取高亮结果
                HighlightField highlightField = highlightFields.get("name");
                if (highlightField != null){
                    // 取出高亮结果数组中的第一个，就是酒店名称
                    String name = highlightField.getFragments()[0].string();
                    hotelDoc.setName(name);
                }
                System.out.println("hotelDoc =" + hotelDoc);
                }
            }

    }

    @BeforeEach
    void setUp(){
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.125.100:9200")
        ));
    }

    @AfterEach
    void tearDown() throws IOException {
        this.client.close();
    }
}
