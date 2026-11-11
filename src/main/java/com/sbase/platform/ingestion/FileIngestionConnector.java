package com.sbase.platform.ingestion;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sbase.platform.IngestionConnector;
import org.apache.commons.csv.*;
import org.springframework.stereotype.Component;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

@Component
public class FileIngestionConnector implements IngestionConnector {
    private static final int BATCH_SIZE=500;
    private final DurableRecordSink sink;
    private final ObjectMapper mapper;
    public FileIngestionConnector(DurableRecordSink sink,ObjectMapper mapper){this.sink=sink;this.mapper=mapper;}
    @Override public String type(){return "file";}

    @Override
    public ConnectorResult ingest(ConnectorRequest request) {
        Map<String,String> o=request.options();
        UUID connectorId=UUID.fromString(required(o,"connectorId"));
        Path path=Path.of(required(o,"path")).toAbsolutePath().normalize();
        Path root=Path.of(required(o,"allowedRoot")).toAbsolutePath().normalize();
        try {
            Path realRoot=root.toRealPath();
            Path realFile=path.toRealPath();
            if(!realFile.startsWith(realRoot)||!Files.isRegularFile(realFile))
                throw new IllegalArgumentException("File must resolve to a regular file under allowedRoot");
            String format=o.getOrDefault("format",extension(realFile)).toLowerCase(Locale.ROOT);
            return switch(format) {
                case "csv" -> ingestCsv(connectorId,realFile,o);
                case "jsonl","ndjson" -> ingestJsonLines(connectorId,realFile);
                default -> throw new IllegalArgumentException("Supported formats: csv, jsonl, ndjson");
            };
        } catch(IOException ex) { throw new IllegalStateException("File ingestion failed",ex); }
    }

    private ConnectorResult ingestCsv(UUID id,Path path,Map<String,String> o)throws IOException {
        boolean header=Boolean.parseBoolean(o.getOrDefault("header","true"));
        String[] supplied=o.containsKey("columns")?Arrays.stream(o.get("columns").split(",")).map(String::trim).toArray(String[]::new):new String[0];
        CSVFormat format=CSVFormat.DEFAULT.builder()
            .setHeader(header?new String[0]:supplied)
            .setSkipHeaderRecord(header)
            .setIgnoreEmptyLines(true)
            .setTrim(false)
            .build();
        long read=0,written=0;
        List<DurableRecordSink.Envelope> batch=new ArrayList<>(BATCH_SIZE);
        try(Reader reader=Files.newBufferedReader(path,StandardCharsets.UTF_8);
            CSVParser parser=format.parse(reader)) {
            if(!header && supplied.length==0) throw new IllegalArgumentException("CSV without a header requires columns");
            List<String> headers=parser.getHeaderNames();
            if(headers.isEmpty()) throw new IllegalArgumentException("CSV header is empty");
            Set<String> unique=new HashSet<>();
            for(String name:headers) if(name==null||name.isBlank()||!unique.add(name)) throw new IllegalArgumentException("CSV headers must be non-empty and unique");
            for(CSVRecord record:parser) {
                if(record.size()!=headers.size()) throw new IllegalArgumentException("CSV column count mismatch at record "+record.getRecordNumber());
                Map<String,Object> row=new LinkedHashMap<>();
                for(int i=0;i<headers.size();i++) row.put(headers.get(i),record.get(i));
                batch.add(new DurableRecordSink.Envelope(path+":"+record.getRecordNumber(),row));
                read++;
                if(batch.size()==BATCH_SIZE){written+=sink.write(id,path.toString(),batch,"file:"+path,String.valueOf(record.getRecordNumber()));batch.clear();}
            }
            written+=sink.write(id,path.toString(),batch,"file:"+path,String.valueOf(parser.getRecordNumber()));
        }
        return new ConnectorResult(read,written,"RFC 4180 CSV records persisted with stable record-number keys");
    }

    private ConnectorResult ingestJsonLines(UUID id,Path path)throws IOException {
        long read=0,written=0,lineNo=0;
        List<DurableRecordSink.Envelope> batch=new ArrayList<>(BATCH_SIZE);
        try(BufferedReader reader=Files.newBufferedReader(path,StandardCharsets.UTF_8)) {
            String line;
            while((line=reader.readLine())!=null) {
                lineNo++;
                if(line.isBlank()) continue;
                Map<String,Object> payload=mapper.readValue(line,new TypeReference<Map<String,Object>>(){});
                batch.add(new DurableRecordSink.Envelope(path+":"+lineNo,payload));read++;
                if(batch.size()==BATCH_SIZE){written+=sink.write(id,path.toString(),batch,"file:"+path,String.valueOf(lineNo));batch.clear();}
            }
            written+=sink.write(id,path.toString(),batch,"file:"+path,String.valueOf(lineNo));
        }
        return new ConnectorResult(read,written,"JSON Lines records persisted with line-level idempotency");
    }

    private static String extension(Path p){String n=p.getFileName().toString();int i=n.lastIndexOf('.');return i<0?"":n.substring(i+1);}
    private static String required(Map<String,String> o,String k){String v=o.get(k);if(v==null||v.isBlank())throw new IllegalArgumentException("Missing option: "+k);return v;}
}
