package cn.itcast.hotel;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static cn.itcast.hotel.constants.HotelConstants.MAPPING_TEMPLATE;

public class HotelIndexTest {
    private RestHighLevelClient client;

    @Test
    void testInit(){
        System.out.println(client);
    }

    @Test
    void testCreateHotelIndex() throws IOException {
        //1.创建Request对象
        CreateIndexRequest request = new CreateIndexRequest("hotel");
        //2.请求参数，MAPPING_TEMPLATE是静态常量字符串，内容是创建索引库的DSL语句
        request.source(MAPPING_TEMPLATE, XContentType.JSON);
        //3.发起请求
        client.indices().create(request, RequestOptions.DEFAULT);
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
