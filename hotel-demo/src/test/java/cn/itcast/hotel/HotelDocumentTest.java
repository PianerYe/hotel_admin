package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    @Test
    void testGetDocumentById() throws IOException {
        //1.准备Request对象
        GetRequest request = new GetRequest("hotel","61083");
        //2。发送请求
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        //3. 解析结果
        String json = response.getSourceAsString();

        HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);

        System.out.println(hotelDoc);
    }

    @Test
    void testUpdateDocument() throws IOException {
        //1.准备Request对象
        UpdateRequest request = new UpdateRequest("hotel","61083");
        //2. 准备一个参数，每两个参数为一对key value
        request.doc(
                "price",952,
                "starName","四钻"
        );
        //3。更新文档
        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
    }

    @Test
    void testDeleteDocumentById() throws IOException{
        DeleteRequest request = new DeleteRequest("hotel","61083");
        client.delete(request,RequestOptions.DEFAULT);
    }

    @Test
    void testBulkRequest() throws IOException {
        // 1.创建Request
        BulkRequest request = new BulkRequest();
        // 2.准备参数，添加多个新增的Request
            // 批量查询酒店数据
        List<Hotel> list = hotelService.list();
            // 转换为文档类型HotelDoc
            //批量添加
        list.stream().map((item) -> {
            HotelDoc hotelDoc = new HotelDoc(item);
            request.add(new IndexRequest("hotel")
                    .id(hotelDoc.getId().toString())
                    .source(JSON.toJSONString(hotelDoc), XContentType.JSON));
            return hotelDoc;
        }).collect(Collectors.toList());
            //批量删除
//        list.stream().map((item) -> {
//            HotelDoc hotelDoc = new HotelDoc(item);
//            request.add(new DeleteRequest("hotel")
//                    .id(hotelDoc.getId().toString()));
//            return hotelDoc;
//        }).collect(Collectors.toList());
        // 3.发送请求
        client.bulk(request,RequestOptions.DEFAULT);
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
