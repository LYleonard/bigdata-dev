import com.alibaba.fastjson.JSONObject;
import io.wrp.Person;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author LYleonard
 * @date 2020/7/30 14:44
 * @description ES javaAPI 操作
 */
public class IndexOperationsTest {

    private TransportClient client;

    @BeforeEach
    public void createClient() throws UnknownHostException {
        Settings settings = Settings.builder().put("cluster.name", "elasticsearch").build();
        client = new PreBuiltTransportClient(settings).addTransportAddress(
                new TransportAddress(InetAddress.getByName("192.168.20.8"), 19300));
    }

    /**
     * 拼接JSON字符串，插入index
     */
    @Test
    public void createIndex(){
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("user", "jymmee");
        jsonObject.put("postDate", "2020-07-30");
        jsonObject.put("message", "api elasticsearch");

        IndexResponse indexResponse = client.prepareIndex("indexjavaapi", "article", "5")
                .setSource(jsonObject).get();
    }

    /**
     * Map类型对象插入index
     */
    @Test
    public void createIndex2(){
        HashMap<String, String> jsonMap = new HashMap<>();
        jsonMap.put("name", "long");
        jsonMap.put("age", "20");
        jsonMap.put("sex", "man");
        jsonMap.put("address", "gz");

        IndexResponse indexResponse = client.prepareIndex("indexjavaapi2", "article", "6")
                .setSource(jsonMap).get();
        client.close();
    }

    /**
     * 通过XContentBuilder来实现索引的创建
     */
    @Test
    public void createIndex3() throws IOException {
        IndexResponse indexResponse = client.prepareIndex("indexjavaapi", "article", "7")
                .setSource(new XContentFactory().jsonBuilder().startObject()
                        .field("user", "liu")
                        .field("age", "23")
                        .field("sex", "woman")
                        .field("address", "beijing")
                        .endObject()
                ).get();
        client.close();
    }

    /**
     * 将java对象转换为json字符串插入索引
     */
    @Test
    public void objToIndex(){
        Person person = new Person();
        person.setId(8);
        person.setName("lisi");
        person.setAge(18);
        person.setSex(1);
        person.setAddress("cd");
        person.setPhone("183xxxxxxxx");
        person.setEmail("lisi@xx.com");
        person.setSay("Hello ES");

        String jsonStr = JSONObject.toJSONString(person);
        IndexResponse indexResponse = client.prepareIndex("indexjavaapi", "article", "8")
                .setSource(jsonStr, XContentType.JSON).get();
        client.close();
    }

    /**
     * 批量创建索引
     */
    @Test
    public void bulkIndex() throws IOException {
        BulkRequestBuilder bulk = client.prepareBulk();
        bulk.add(client.prepareIndex("indexjavaapi", "article", "9")
                .setSource(new XContentFactory().jsonBuilder().startObject()
                        .field("user", "chen")
                        .field("age", "22")
                        .field("sex", "woman")
                        .field("address", "km")
                        .endObject())
        );
        bulk.add(client.prepareIndex("indexjavaapi", "article", "10")
                .setSource(new XContentFactory().jsonBuilder().startObject()
                        .field("user", "wenwen")
                        .field("age", "21")
                        .field("sex", "woman")
                        .field("address", "wh")
                        .endObject())
        );
        BulkResponse bulkResponse = bulk.get();
        client.close();
    }

    /**
     * 初始化一批数据到索引库中准备查询
     * @throws IOException
     */
    @Test
    public void createIndexBatch() throws IOException {
        //创建映射
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("properties")
                .startObject("id").field("type", "integer").endObject()
                .startObject("name").field("type", "text")
                .field("analyzer", "ik_max_word").endObject()
                .startObject("age").field("type", "integer").endObject()
                .startObject("sex").field("type", "text")
                .field("analyzer","ik_max_word").endObject()
                .startObject("address").field("type", "text")
                .field("analyzer", "ik_max_word").endObject()
                .startObject("phone").field("type", "text").endObject()
                .startObject("email").field("type", "text").endObject()
                .startObject("say").field("type", "text")
                .field("analyzer", "ik_max_word").endObject().endObject().endObject();

        //indexsearch：索引名   mysearch：类型名（可以自己定义）
        PutMappingRequest putMap = Requests.putMappingRequest("indexsearch")
                .type("mysearch").source(mapping);
        //创建索引
        client.admin().indices().prepareCreate("indexsearch").execute().actionGet();
        //为索引添加映射
        client.admin().indices().putMapping(putMap).actionGet();

        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        Person lujunyi = new Person(2, "玉麒麟卢俊义", 28, 1, "水泊梁山", "17666666666", "lujunyi@163.com","hello world今天天气还不错");
        Person wuyong = new Person(3, "智多星吴用", 45, 1, "水泊梁山", "17666666666", "wuyong@163.com","行走四方，抱打不平");
        Person gongsunsheng = new Person(4, "入云龙公孙胜", 30, 1, "水泊梁山", "17666666666", "gongsunsheng@163.com","走一个");
        Person guansheng = new Person(5, "大刀关胜", 42, 1, "水泊梁山", "17666666666", "wusong@163.com","我的大刀已经饥渴难耐");
        Person linchong = new Person(6, "豹子头林冲", 18, 1, "水泊梁山", "17666666666", "linchong@163.com","梁山好汉");
        Person qinming = new Person(7, "霹雳火秦明", 28, 1, "水泊梁山", "17666666666", "qinming@163.com","不太了解");
        Person huyanzhuo = new Person(8, "双鞭呼延灼", 25, 1, "水泊梁山", "17666666666", "huyanzhuo@163.com","不是很熟悉");
        Person huarong = new Person(9, "小李广花荣", 50, 1, "水泊梁山", "17666666666", "huarong@163.com","打酱油的");
        Person chaijin = new Person(10, "小旋风柴进", 32, 1, "水泊梁山", "17666666666", "chaijin@163.com","吓唬人的");
        Person zhisheng = new Person(13, "花和尚鲁智深", 15, 1, "水泊梁山", "17666666666", "luzhisheng@163.com","倒拔杨垂柳");
        Person wusong = new Person(14, "行者武松", 28, 1, "水泊梁山", "17666666666", "wusong@163.com","二营长。。。。。。");

        bulkRequestBuilder.add(client.prepareIndex("indexsearch", "mysearch", "1")
                .setSource(JSONObject.toJSONString(lujunyi), XContentType.JSON));
        bulkRequestBuilder.add(client.prepareIndex("indexsearch", "mysearch", "2")
                .setSource(JSONObject.toJSONString(wuyong), XContentType.JSON));
        bulkRequestBuilder.add(client.prepareIndex("indexsearch", "mysearch", "3")
                .setSource(JSONObject.toJSONString(gongsunsheng), XContentType.JSON));
        bulkRequestBuilder.add(client.prepareIndex("indexsearch", "mysearch", "4")
                .setSource(JSONObject.toJSONString(guansheng), XContentType.JSON));
        bulkRequestBuilder.add(client.prepareIndex("indexsearch", "mysearch", "5")
                .setSource(JSONObject.toJSONString(linchong), XContentType.JSON));
        bulkRequestBuilder.add(client.prepareIndex("indexsearch", "mysearch", "6")
                .setSource(JSONObject.toJSONString(qinming), XContentType.JSON));
        bulkRequestBuilder.add(client.prepareIndex("indexsearch", "mysearch", "7")
                .setSource(JSONObject.toJSONString(huyanzhuo), XContentType.JSON));
        bulkRequestBuilder.add(client.prepareIndex("indexsearch", "mysearch", "8")
                .setSource(JSONObject.toJSONString(huarong), XContentType.JSON));
        bulkRequestBuilder.add(client.prepareIndex("indexsearch", "mysearch", "9")
                .setSource(JSONObject.toJSONString(chaijin), XContentType.JSON));
        bulkRequestBuilder.add(client.prepareIndex("indexsearch", "mysearch", "10")
                .setSource(JSONObject.toJSONString(zhisheng), XContentType.JSON));
        bulkRequestBuilder.add(client.prepareIndex("indexsearch", "mysearch", "11")
                .setSource(JSONObject.toJSONString(wusong), XContentType.JSON));
        bulkRequestBuilder.get();
        client.close();
    }


    /**
     * 通过id来进行精确查询
     */
    @Test
    public void quertById(){
        GetResponse documentFields = client.prepareGet("indexsearch", "mysearch", "6").get();
        String index = documentFields.getIndex();
        String type = documentFields.getType();
        String id = documentFields.getId();
        System.out.println("index: " + index);
        System.out.println("type: " + type);
        System.out.println("id: " + id);
        Map<String, Object> source = documentFields.getSource();
        for (String s: source.keySet()){
            System.out.println(source.get(s));
        }
    }

    /**
     * 查询所有数据
     */
    @Test
    public void queryAll(){
        SearchResponse searchResponse = client.prepareSearch("indexsearch").setTypes("mysearch")
                .setQuery(new MatchAllQueryBuilder()).get();
        SearchHits searchHits = searchResponse.getHits();
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
        }
        client.close();
    }

    /**
     * 范围查询: 查找年龄18到28的人,包含18和28
     */
    @Test
    public void rangeQuery() {
        SearchResponse searchResponse = client.prepareSearch("indexsearch")
                .setTypes("mysearch").setQuery(new RangeQueryBuilder("age").gt(17).lt(29))
                .get();
        SearchHits hits = searchResponse.getHits();
        SearchHit[] hit = hits.getHits();
        for (SearchHit documentFields: hit){
            System.out.println(documentFields.getSourceAsString());
        }
        client.close();
    }

    /**
     * termQuery词条查询
     */
    @Test
    public void termQuery(){
        SearchResponse searchResponse = client.prepareSearch("indexsearch")
                .setTypes("mysearch").setQuery(
                        new TermQueryBuilder("say", "熟悉")).get();
        SearchHits hits = searchResponse.getHits();
        SearchHit[] hit = hits.getHits();
        for (SearchHit documentFields : hit) {
            System.out.println(documentFields.getSourceAsString());
        }
        client.close();
    }

    /**
     * fuzzyQuery模糊查询: fuzzyQuery表示英文单词的最大可纠正次数，最大可以自动纠正两次
     */
    @Test
    public void fuzzyQuery(){
        SearchResponse searchResponse = client.prepareSearch("indexsearch")
                .setTypes("mysearch").setQuery(QueryBuilders
                        .fuzzyQuery("say", "helOL").fuzziness(Fuzziness.TWO))
                .get();

        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit: hits.getHits()){
            System.out.println(hit.getSourceAsString());
        }
        client.close();
    }

    /**
     * wildCardQuery通配符查询: 模糊匹配查询有两种匹配符，分别是" * " 以及 " ? "，
     * 用" * "来匹配任何字符，包括空字符串。
     * 用" ? "来匹配任意的单个字符
     */
    @Test
    public void wildCardQuery(){
        SearchResponse searchResponse = client.prepareSearch("indexsearch")
                .setTypes("mysearch").setQuery(QueryBuilders
                        .wildcardQuery("say", "hel*"))
                .get();
        SearchHits hits = searchResponse.getHits();
        SearchHit[] hits1 = hits.getHits();
        for (SearchHit hit : hits1) {
            System.out.println(hit.getSourceAsString());
        }
        client.close();
    }

    /**
     * boolQuery 多条件组合查询: 查询年龄是18到28范围内且性别是男性的，或者id范围在10到13范围内的
     */
    @Test
    public void boolQueryTest(){
        RangeQueryBuilder age = QueryBuilders.rangeQuery("age").gt(17).lt(19);
        TermQueryBuilder sex = QueryBuilders.termQuery("sex", "1");
        RangeQueryBuilder id = QueryBuilders.rangeQuery("id").gt("9").lt("14");

        SearchResponse searchResponse = client.prepareSearch("indexsearch")
                .setTypes("mysearch").setQuery(QueryBuilders.boolQuery()
                        .should(id).should(QueryBuilders.boolQuery().must(age).must(sex)))
                .get();
        SearchHits hits = searchResponse.getHits();
        SearchHit[] hits1 = hits.getHits();
        for (SearchHit documentFields : hits1) {
            System.out.println(documentFields.getSourceAsString());
        }
        client.close();
    }

    /**
     * 分页查询
     */
    @Test
    public void getPageIndex(){
        int pageSize = 5;
        int pageNum = 2;
        int startNum = (pageNum - 1) * 5;
        SearchResponse searchResponse = client.prepareSearch("indexsearch")
                .setTypes("mysearch").setQuery(QueryBuilders.matchAllQuery())
                .addSort("id", SortOrder.ASC).setFrom(startNum).setSize(pageSize).get();
        SearchHits hits = searchResponse.getHits();
        SearchHit[] hits1 = hits.getHits();
        for (SearchHit documentFields : hits1) {
            System.out.println(documentFields.getSourceAsString());
        }
        client.close();
    }

    /**
     * 高亮查询
     */
    @Test
    public void highLight() {
        // 设置查询需要高亮的字段
        SearchRequestBuilder requestBuilder = client.prepareSearch("indexsearch")
                .setTypes("mysearch").setQuery(QueryBuilders.termQuery("say", "hello"));
        // 设置字段高亮的前缀和后缀
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("say").preTags("<front style='color:red'").postTags("</front>");

        //通过高亮查询数据
        SearchResponse searchResponse = requestBuilder.highlighter(highlightBuilder).get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("查询共：" + hits.getTotalHits() + "条数据！");
        for (SearchHit hit : hits) {
            // 打印没有高亮显示的数据
            System.out.println(hit.getSourceAsString());
            System.out.println("++++++++++++++++++++++");

            //打印高亮显示的数据
            Text[] says = hit.getHighlightFields().get("say").getFragments();
            for (Text say : says) {
                System.out.println("say:" + say);
            }
        }
        client.close();
    }

    /**
     * 更新索引
     */
    @Test
    public void updateIndex() {
        Person person = new Person(5, "爱因斯坦", 99, 1, "卡洛杉矶",
                "18399998888", "aiyin@qq.com", "广义相对论");
        client.prepareUpdate().setIndex("indexsearch").setType("mysearch").setId("5")
                .setDoc(JSONObject.toJSONString(person), XContentType.JSON).get();

        client.close();
    }


    /**
     * 删除索引
     */
    @Test
    public void deleteById() {
        DeleteResponse deleteResponse = client.prepareDelete("indexsearch", "mysearch", "11").get();
        client.close();
    }

    /**
     * 删除整个索引库
     */
    @Test
    public  void  deleteIndex(){
        AcknowledgedResponse indexsearch = client.admin().indices().prepareDelete("blog01").execute().actionGet();
        client.close();
    }


}
