package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

@SpringBootTest
public class HotelDocumentTest {
    @Autowired
    private IHotelService hotelService;
    private RestHighLevelClient client;

    @Test
    void testAddDocument() throws IOException {
        // 根据id查询
        Hotel hotel = hotelService.getById(61083);
        // 转换为文档类型
        HotelDoc hotelDoc = new HotelDoc(hotel);
        //1.准备Request对象
        IndexRequest request = new IndexRequest("hotel").id(hotel.getId().toString());
        //2.准备json文档
        request.source(JSON.toJSONString(hotelDoc), XContentType.JSON);
        //3。发送请求
        client.index(request, RequestOptions.DEFAULT);
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
