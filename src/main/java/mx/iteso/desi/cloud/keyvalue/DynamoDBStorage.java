package mx.iteso.desi.cloud.keyvalue;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;
import mx.iteso.desi.cloud.lp1.Config;

import java.util.*;


public class DynamoDBStorage extends BasicKeyValueStore {

    String dbName;
    AmazonDynamoDB ddbClient = AmazonDynamoDBClientBuilder.standard().withRegion(Config.amazonRegion).build();
    // Simple autoincrement counter to make sure we have unique entries
    int inx;

    Set<String> attributesToGet = new HashSet<String>();

    private Map<String,Integer> lastIndexes = new HashMap<>();

    public DynamoDBStorage(String dbName) {
        this.dbName = dbName;
        init();
    }

    private void init(){
        if(tableExists(this.dbName))return;

        createTable(this.dbName,"keyword","S","inx","N");
    }
    private boolean tableExists(String tableName){
        ListTablesResult res = ddbClient.listTables();
        for(String tName : res.getTableNames()){
            if(tName.equalsIgnoreCase(tableName)){
                return true;
            }
        }
        return false;
    }
    private boolean createTable(String name,String partitionKeyName, String partitionKeyType,
                                String sortKeyName, String sortKeyType){

        try {
            List<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();

        /* Partition key */
            keySchema.add(new KeySchemaElement()
                    .withAttributeName(partitionKeyName)
                    .withKeyType(KeyType.HASH));

            ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
            attributeDefinitions.add(new AttributeDefinition()
                    .withAttributeName(partitionKeyName)
                    .withAttributeType(partitionKeyType));

        /* Sort key */
            if (sortKeyName != null) {
                keySchema.add(new KeySchemaElement()
                        .withAttributeName(sortKeyName)
                        .withKeyType(KeyType.RANGE));
                attributeDefinitions.add(new AttributeDefinition()
                        .withAttributeName(sortKeyName)
                        .withAttributeType(sortKeyType));
            }

            ProvisionedThroughput throughput = new ProvisionedThroughput();
            throughput.setReadCapacityUnits(1L);
            throughput.setWriteCapacityUnits(1L);

            CreateTableRequest createTableRequest = new CreateTableRequest()
                    .withTableName(name)
                    .withProvisionedThroughput(throughput)
                    .withKeySchema(keySchema);

            createTableRequest.setAttributeDefinitions(attributeDefinitions);

            CreateTableResult result = ddbClient.createTable(createTableRequest);
            if(result!=null)return true;

        }
        catch(Exception e){
            System.err.println(e);
        }
        return false;
    }

    private int getLastIdx(String keyword){

        Map<String, AttributeValue> expressionAttributeValues =
                new HashMap<String, AttributeValue>();
        expressionAttributeValues.put(":val", new AttributeValue().withS(keyword));

        ScanRequest scanRequest = new ScanRequest()
                .withTableName(this.dbName)
                .withFilterExpression("keyword = :val")
                .withProjectionExpression("keyword,inx")
                .withExpressionAttributeValues(expressionAttributeValues);


        ScanResult result = this.ddbClient.scan(scanRequest);
        int highest = 0;
        for (Map<String, AttributeValue> item : result.getItems()) {
            int temp = Integer.parseInt(item.get("inx").getN());
            if(temp>highest)highest=temp;
        }
        return highest;
    }
    @Override
    public Set<String> get(String search) {
        //if(!exists(search))return null;
        try{
            Map<String, Condition> keyConditions = new HashMap<>();
            keyConditions.put("keyword",new Condition()
                    .withComparisonOperator(ComparisonOperator.EQ)
                    .withAttributeValueList(new AttributeValue().withS(search))
            );
            QueryRequest qr = new QueryRequest().withTableName(this.dbName).withKeyConditions(keyConditions);
            QueryResult query = this.ddbClient.query(qr);
            List<Map<String,AttributeValue>> queryResult = query.getItems();
            Set<String> set = new HashSet<>();
            for(Map<String,AttributeValue> row : query.getItems()) {
                for (Map.Entry<String, AttributeValue> item : row.entrySet()) {
                    String attributeName = item.getKey();
                    AttributeValue value = item.getValue();
                    if (attributeName.equals("value")) {
                        set.add(value.getS());
                    }
                }
            }
            return set;
        }catch(Exception e){

            return null;
        }

    }

    @Override
    public boolean exists(String search) {
        int last = getLastIdx(search);
        return (last>0);
    }

    @Override
    public Set<String> getPrefix(String search) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void addToSet(String keyword, String value) {
        put(keyword,value);
    }

    @Override
    public void put(String keyword, String value) {
        //int lastInx = getLastIdx(keyword);
        int lastInx;
        if(this.lastIndexes.containsKey(keyword)) {
            lastInx = this.lastIndexes.get(keyword);
            lastInx++;
        }else{
            lastInx = 1;
            this.lastIndexes.put(keyword,lastInx);
        }
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("keyword", new AttributeValue().withS(keyword));
        item.put("inx", new AttributeValue().withN(Integer.toString(lastInx)));
        item.put("value", new AttributeValue().withS(value));
        PutItemRequest putItemRequest = new PutItemRequest(this.dbName, item);
        PutItemResult putItemResult = this.ddbClient.putItem(putItemRequest);
    }

    @Override
    public void close() {
        this.ddbClient.shutdown();
    }
    
    @Override
    public boolean supportsPrefixes() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void sync() {
    }

    @Override
    public boolean isCompressible() {
        return false;
    }

    @Override
    public boolean supportsMoreThan256Attributes() {
        return true;
    }

}
