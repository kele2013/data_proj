package org.apache.nifi.processor.StreamLoad;

import com.alibaba.fastjson.JSON;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import javax.security.auth.login.LoginException;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import static org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;

/**
 * Created by coco1 on 2017/7/18.
 */
@SideEffectFree
@Tags({"streamload"})
@CapabilityDescription("Fetch value from nebula.")
public class StreamLoadProcessor extends AbstractProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(StreamLoadProcessor.class);

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    private volatile int ffbatch = 1;

    private StarRocksStreamLoad starrocksStreamLoad;

    private String tableDb;


    private String tableName;

    private String strocksAddress;

    private String filedlist;

    private String exclude_fileds;

    private String load_type;

    private String talbe_type;


    public static final PropertyDescriptor JSON_PATH = new PropertyDescriptor.Builder()
            .name("JqPath")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    public static final PropertyDescriptor TOPIC = new PropertyDescriptor.Builder()
            .name("Topic Name")
            .description("The Redis key to watch for work")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();


    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Succes relationship")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("FALIURE")
            .description("A FlowFile is routed to this relationship if it cannot be converted into a SQL statement. Common causes include invalid JSON "
                    + "content or the JSON content missing a required field (if using an INSERT statement type).")
            .build();

    protected static final PropertyDescriptor FLOWFILE_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("FlowFiles per Batch")
            .description("The maximum number of FlowFiles to process in a single execution, between 1 - 100000. " +
                    "Depending on your memory size, and data size per row set an appropriate batch size " +
                    "for the number of FlowFiles to process per client connection setup." +
                    "Gradually increase this number, only if your FlowFiles typically contain a few records.")
            .defaultValue("1")
            .required(true)
            .addValidator(StandardValidators.createLongValidator(1, 100000, true))
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("The service for reading records from incoming flow files.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    protected static final PropertyDescriptor STARROCKS_DB = new PropertyDescriptor.Builder()
            .name("starrocks-dmc-table-DB")
            .description("The name of the Kudu Table to put data into")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .build();


    public static final PropertyDescriptor STARROCKS_TABLE = new PropertyDescriptor.Builder()
            .name("starrocks-dmc-table-name")
            .displayName("Table Name")
            .description("The name of the table where the cache will be stored.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor FEILDLIST_ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
            .name("starrocks-dmc-table-feildlist")
            .displayName("Table feild list")
            .description("The list of the starrocks table filed.")
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    public static final PropertyDescriptor EXCLUDE_FILEDS = new PropertyDescriptor.Builder()
            .name("starrocks-exclude-feildlist")
            .displayName("exclude Table feild list")
            .description("The list of the starrocks table excluded.")
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();
    public static final PropertyDescriptor STARROCKS_ADDRESS = new PropertyDescriptor.Builder()
            .name("starrocks-address")
            .displayName("address")
            .description("The name of the address where the cache will be stored.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    public static final PropertyDescriptor STARROCKS_HTTP_PORT = new PropertyDescriptor.Builder()
            .name("starrocks-port")
            .displayName("port")
            .description("The port of the address where the cache will be stored.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();
    public static final PropertyDescriptor LOAD_JSON_OR_ATTRBUITE = new PropertyDescriptor.Builder()
            .name("load-json-or-flowfile-attribute")
            .displayName("load_type")
            .description("The type of the load streamload to starocks.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    public static final PropertyDescriptor TABLE_TYPE = new PropertyDescriptor.Builder()
            .name("load-table-type")
            .displayName("load_type")
            .description("The type of the load table is primary or arregate.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    @Override
    public Set<Relationship> getRelationships() {

        return this.relationships;
    }

    @Override
    public void init(final ProcessorInitializationContext context) {

        getLogger().info("nebula processor init.");
        ArrayList<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(STARROCKS_TABLE);
        properties.add(STARROCKS_DB);
        properties.add(FLOWFILE_BATCH_SIZE);
        properties.add(LOAD_JSON_OR_ATTRBUITE);
        // properties.add(FEILDLIST_ATTRIBUTE_NAME);
        properties.add(STARROCKS_ADDRESS);
        properties.add(STARROCKS_HTTP_PORT);
        properties.add(EXCLUDE_FILEDS);
        properties.add(TABLE_TYPE);
        //properties.add(RECORD_READER);


//        防止多线程ADD
        this.properties = Collections.unmodifiableList(properties);
        Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(REL_FAILURE);
//        防止多线程ADD
        this.relationships = Collections.unmodifiableSet(relationships);


        this.starrocksStreamLoad = new StarRocksStreamLoad();


        //createSessionPool((ProcessContext) context);
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws LoginException {
        this.tableDb = context.getProperty(STARROCKS_DB).evaluateAttributeExpressions().toString();
        //this.tableName  = context.getProperty(STARROCKS_TABLE).evaluateAttributeExpressions().toString();

        this.ffbatch = context.getProperty(FLOWFILE_BATCH_SIZE).evaluateAttributeExpressions().asInteger();
        LOG.info(" StreamLoadProcessor onScheduled tableDb: {},tableName:{}", tableDb, tableName);

        this.strocksAddress = context.getProperty(STARROCKS_ADDRESS).evaluateAttributeExpressions().toString();
        int stark_port = context.getProperty(STARROCKS_HTTP_PORT).evaluateAttributeExpressions().asInteger();

        filedlist = context.getProperty(FEILDLIST_ATTRIBUTE_NAME).getValue();
        load_type = context.getProperty(LOAD_JSON_OR_ATTRBUITE).getValue();
        exclude_fileds = context.getProperty(EXCLUDE_FILEDS).getValue();

        talbe_type = context.getProperty(TABLE_TYPE).getValue();

        starrocksStreamLoad.setSTARROCKS_HOST(strocksAddress);
        starrocksStreamLoad.setSTARROCKS_HTTP_PORT(stark_port);
        starrocksStreamLoad.SetSTARROCKS_DB(tableDb);
        starrocksStreamLoad.SetSTARROCKS_TABLE(tableName);
        starrocksStreamLoad.setTABLETYPE(talbe_type);


        LOG.info(" StreamLoadProcessor onScheduled strocksAddress: {},stark_port:{}", strocksAddress, stark_port);


    }

    private String getEvaluatedProperty(PropertyDescriptor property, ProcessContext context, FlowFile flowFile) {
        PropertyValue evaluatedProperty = context.getProperty(property).evaluateAttributeExpressions(flowFile);
        if (property.isRequired() && evaluatedProperty == null) {
            throw new ProcessException(String.format("Property `%s` is required but evaluated to null", property.getDisplayName()));
        }
        return evaluatedProperty.getValue();
    }
    public static boolean isNumeric(String str) {
        Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
        return pattern.matcher(str).matches();
    }

    private String transferUSDate(String dateStr) throws ParseException {
    //String dateStr = "Fri Feb 19 17:32:34CST 2021";
        Date date = null;

        //Calendar cal = Calendar.getInstance();
        //cal.setTime(date);//date 换成已经已知的Date对象
        //cal.add(Calendar.HOUR_OF_DAY, -8);

        if(isNumeric(dateStr)){
             date = new Date(Long.parseLong(dateStr));
        }
        else if(dateStr.indexOf("CST")>0){
            SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);
            date = sdf.parse(dateStr);

        }
        else {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            date = sdf.parse(dateStr);
        }

    String formatStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);
    return formatStr;
}

/*
    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {
        final List<FlowFile> flowFiles = processSession.get(ffbatch);
        if (flowFiles.isEmpty()) {
            getLogger().error("StreamLoadProcessor onTrigger flowFiles size is null");
            return;
        }

        final RecordReaderFactory recordReaderFactory = processContext.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        for (FlowFile flowFile : flowFiles) {
            final String tableName = getEvaluatedProperty(STARROCKS_TABLE, processContext, flowFile);
            final InputStream in = processSession.read(flowFile);
            try {
                final RecordReader recordReader = recordReaderFactory.createRecordReader(flowFile, in, getLogger());
                final RecordSet recordSet = recordReader.createRecordSet();
                Record record = recordSet.next();
                recordReaderLoop: while (record != null) {
                 final List<String> fieldNames = record.getSchema().getFieldNames();
                    for (final String field : fieldNames) {
                        Object value = record.getValue(field);
                        getLogger().info("StreamLoadProcessor onTrigger fieldName:{} ,value: {}", field,value);
                    }
                }


            } catch (MalformedRecordException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (SchemaNotFoundException e) {
                throw new RuntimeException(e);
            }
        }


    }
    */


    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {


        final List<FlowFile> flowFiles = processSession.get(ffbatch);
        if (flowFiles.isEmpty()) {
            getLogger().error("StreamLoadProcessor onTrigger flowFiles size is null");
            return;
        }

           getLogger().info("StreamLoadProcessor onTrigger flowFiles size={}", flowFiles.size());

          Map<String,StringBuilder> table_Fileds=new HashMap<>();

          Map<String,StringBuilder> table_valueList= new HashMap<String, StringBuilder>();

            StringBuilder fieldListStr = new StringBuilder();
            for (final FlowFile flowFile : flowFiles) {


                final String tableName = getEvaluatedProperty(STARROCKS_TABLE, processContext, flowFile);

              //  starrocksStreamLoad.SetSTARROCKS_TABLE(tableName);

                getLogger().info("StreamLoadProcessor onTrigger table_name:{} ,filedlist: {}", tableName,filedlist);

                final AtomicReference<StringBuilder> input = new AtomicReference<>();
                //StringBuilder input= new StringBuilder();
                processSession.read(flowFile, in -> {
                    final InputStream bin = new BufferedInputStream(in);
                    try {

                        input.set(new StringBuilder(IOUtils.toString(bin, String.valueOf(Charset.defaultCharset()))));
                        LOG.info(" StreamLoadProcessor onTrigger bin toString is {}", input.get());

                        // value.set(result);

                    } catch (IOException ex) {
                        ex.printStackTrace();
                        getLogger().error("Failed to read json string.");

                    } finally {
                        bin.close();
                    }
                });


                    StringBuilder filedlistbuf= new StringBuilder();
                  //  if(filedlist==null) {

                        Iterator<String> keys = JSON.parseObject(String.valueOf(input.get())).keySet().iterator();
                        getLogger().info("StreamLoadProcessor onTrigger exclude_fileds {}", exclude_fileds);
                        while (keys.hasNext()) {
                            String key = keys.next();

                            if (key.equalsIgnoreCase("database") || key.equalsIgnoreCase("table_name"))
                                continue;
                            if(exclude_fileds==null)
                                exclude_fileds="";
                            String[] excludeField = exclude_fileds.split(",");
                            boolean isContainExcluded=false;
                            for(int j=0;j<excludeField.length;j++) {
                                if (key.equalsIgnoreCase(excludeField[j]))
                                    isContainExcluded=true;
                            }
                            if(isContainExcluded==false)
                                filedlistbuf.append(key.toString()).append(",");
                        }
                        if(filedlistbuf.charAt(filedlistbuf.length()-1)==',')
                            filedlistbuf.deleteCharAt(filedlistbuf.length()-1);

                        filedlist=filedlistbuf.toString();
                   // }

                    table_Fileds.put(tableName,filedlistbuf);
                    getLogger().info("StreamLoadProcessor onTrigger filedlistbuf {}", filedlistbuf.toString());
                    String[] field = filedlistbuf.toString().split(",");

                    getLogger().info("StreamLoadProcessor onTrigger filedlist size:{}", field.length);

                StringBuilder oneItemfield = new StringBuilder();
                for(int i=0;i<field.length;i++){

                        String fileldAttr = null;
                        if (load_type.equals("json")) {

                            fileldAttr = JSON.parseObject(String.valueOf(input.get())).getString(field[i]);
                        } else {
                            fileldAttr = flowFile.getAttribute(field[i]);
                        }


                        if (fileldAttr == null) {
                            getLogger().info("StreamLoadProcessor onTrigger fileldAttr:{} is null",field[i]);
                           // processSession.transfer(flowFiles, REL_FAILURE);
                           // return;
                        }
                        else{
                            if(field[i].indexOf("time")>0) {
                                try {
                                    fileldAttr = transferUSDate(fileldAttr.trim());
                                } catch (ParseException e) {
                                    getLogger().error("StreamLoadProcessor onTrigger fileld {} transferUSDate error",field[i]);
                                    throw new RuntimeException(e);
                                }
                            }
                            else {

                                if (fileldAttr.contains("\n")) {
                                    getLogger().info("StreamLoadProcessor onTrigger fileld {} contains line break22", field[i]);
                                    fileldAttr=fileldAttr.replaceAll("\\r?\\n", ",");
                                }

                                fileldAttr = fileldAttr.trim();
                            }



                        }


                        getLogger().info("StreamLoadProcessor onTrigger field={},fileldAttr={}", field[i], fileldAttr);

                        oneItemfield.append(fileldAttr);
                        if (i < field.length-1 ) {
                            oneItemfield.append("\t");
                        }
                        if(i == field.length-1 )
                            oneItemfield.append("\n");

                    }

                getLogger().info("StreamLoadProcessor onTrigger oneItemfield={}", oneItemfield.toString());
               //由于每个表不一样,批量处理时filed和value存到map里
                if(table_valueList.get(tableName)!=null)
                    table_valueList.get(tableName).append(oneItemfield);
                else
                    table_valueList.put(tableName,oneItemfield);

                //  fieldListStr.append("\n");



            }//end for FlowFile


                try {
                    for (String key:table_valueList.keySet()) {
                        Date date = new Date();
                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddhhmmss");

                        UUID uuid = UUID.randomUUID();

                        String loadLabel = dateFormat.format(date)+uuid.toString();

                        starrocksStreamLoad.SetSTARROCKS_TABLE(key);
                        StringBuilder fileds=table_Fileds.get(key);
                        StringBuilder  values=table_valueList.get(key);

                        if(fileds==null)
                        {
                            LOG.error("StreamLoadProcessor onTrigger load data get fileds  is null");
                            processSession.transfer(flowFiles, REL_FAILURE);
                            return;
                        }

                        if(values==null)
                        {
                            LOG.error("StreamLoadProcessor onTrigger load data get values is null");
                            processSession.transfer(flowFiles, REL_FAILURE);
                            return;
                        }
                        getLogger().info("StreamLoadProcessor onTrigger get map key:{}, fileds:{},values:{}",key, fileds.toString(),values.toString());
                        getLogger().info("StreamLoadProcessor onTrigger loadLabel:{}", loadLabel);

                        String resp = starrocksStreamLoad.sendData(values.toString(), loadLabel, fileds.toString());

                        getLogger().info("StreamLoadProcessor onTrigger resp:{}", resp);
                        String Status = JSON.parseObject(resp).getString("Status");
                        getLogger().info("StreamLoadProcessor onTrigger Status:{}", Status);
                        if (Status.equalsIgnoreCase("Fail")) {
                            LOG.error("StreamLoadProcessor onTrigger starrocksStreamLoad.sendData error");
                            processSession.transfer(flowFiles, REL_FAILURE);
                            //System.exit(1);
                            return;
                        }
                    }

                    processSession.transfer(flowFiles, SUCCESS);

                } catch (Exception e) {
                    getLogger().error("StreamLoadProcessor starrocksStreamLoad sendData exception");
                    throw new RuntimeException(e);
                }


    }



    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors(){
        return properties;
    }
}
