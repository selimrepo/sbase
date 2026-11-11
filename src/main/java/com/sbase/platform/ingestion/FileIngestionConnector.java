package com.sbase.platform.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sbase.platform.IngestionConnector;
import org.springframework.stereotype.Component;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

@Component
public class FileIngestionConnector implements IngestionConnector {
    private final DurableRecordSink sink; private final ObjectMapper mapper;
    public FileIngestionConnector(DurableRecordSink sink,ObjectMapper mapper){this.sink=sink;this.mapper=mapper;}
    @Override public String type(){return "file";}
    @Override public ConnectorResult ingest(ConnectorRequest request){
        Map<String,String> o=request.options();UUID id=UUID.fromString(required(o,"connectorId"));
        Path path=Path.of(required(o,"path")).toAbsolutePath().normalize();
        String root=required(o,"allowedRoot");
        Path allowed=Path.of(root).toAbsolutePath().normalize();
        if(!path.startsWith(allowed)||!Files.isRegularFile(path))throw new IllegalArgumentException("File must be a regular file under allowedRoot");
        String format=o.getOrDefault("format",extension(path)).toLowerCase(Locale.ROOT);
        boolean header=Boolean.parseBoolean(o.getOrDefault("header","true"));
        List<String> columns=o.containsKey("columns")?Arrays.stream(o.get("columns").split(",")).map(String::trim).toList():List.of();
        long read=0,written=0,lineNo=0;List<DurableRecordSink.Envelope> batch=new ArrayList<>();
        try(BufferedReader reader=Files.newBufferedReader(path,StandardCharsets.UTF_8)){
            String line;
            while((line=reader.readLine())!=null){lineNo++;if(line.isBlank())continue;
                Map<String,Object> payload;
                if(format.equals("jsonl")||format.equals("ndjson"))payload=mapper.readValue(line,Map.class);
                else if(format.equals("csv")){
                    if(lineNo==1&&header){columns=Arrays.stream(line.split(",",-1)).map(String::trim).toList();continue;}
                    String[] vals=line.split(",",-1);
                    if(columns.isEmpty()||columns.size()!=vals.length)throw new IllegalArgumentException("CSV column count mismatch at line "+lineNo);
                    payload=new LinkedHashMap<>();for(int i=0;i<columns.size();i++)payload.put(columns.get(i),vals[i]);
                }else throw new IllegalArgumentException("Supported file formats: csv, jsonl, ndjson");
                batch.add(new DurableRecordSink.Envelope(path+":"+lineNo,payload));read++;
                if(batch.size()==500){written+=sink.write(id,path.toString(),batch,"file:"+path,String.valueOf(lineNo));batch.clear();}
            }
            written+=sink.write(id,path.toString(),batch,"file:"+path,String.valueOf(lineNo));
        }catch(Exception e){throw new IllegalStateException("File ingestion failed",e);}
        return new ConnectorResult(read,written,"File rows persisted with line-level idempotency");
    }
    private static String extension(Path p){String n=p.getFileName().toString();int i=n.lastIndexOf('.');return i<0?"":n.substring(i+1);}
    private static String required(Map<String,String> o,String k){String v=o.get(k);if(v==null||v.isBlank())throw new IllegalArgumentException("Missing option: "+k);return v;}
}
