package com.sbase.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;
import java.util.List;
import java.util.Set;
import java.util.UUID;

@Service
public class PipelineService {
    private static final Set<String> SOURCES=Set.of("read-doris","read-parquet","read-csv");
    private static final Set<String> TRANSFORMS=Set.of("filter","select","derive","aggregate");
    private static final Set<String> SINKS=Set.of("write-doris","write-parquet");
    private final PipelineRepository repository;
    private final ObjectMapper mapper;
    public PipelineService(PipelineRepository repository,ObjectMapper mapper){this.repository=repository;this.mapper=mapper;}

    @Transactional
    public PipelineEntity create(PipelineRequest request){
        validateDefinition(request.definitionJson());
        return repository.save(new PipelineEntity(request.name(),request.description(),request.definitionJson(),request.enabled()));
    }
    @Transactional(readOnly=true)
    public List<PipelineEntity> list(){return repository.findAllByOrderByUpdatedAtDesc();}
    @Transactional(readOnly=true)
    public PipelineEntity get(UUID id){return repository.findById(id).orElseThrow(()->new ResponseStatusException(HttpStatus.NOT_FOUND,"Pipeline not found"));}
    @Transactional
    public PipelineEntity update(UUID id,PipelineRequest request){
        validateDefinition(request.definitionJson());
        PipelineEntity pipeline=get(id);
        pipeline.update(request.name(),request.description(),request.definitionJson(),request.enabled());
        return pipeline;
    }
    @Transactional
    public void delete(UUID id){repository.delete(get(id));}

    void validateDefinition(String json){
        try{
            JsonNode root=mapper.readTree(json);
            if(root==null||!root.isObject()||!root.path("steps").isArray())
                throw new IllegalArgumentException("Pipeline definition must contain a steps array");
            JsonNode steps=root.path("steps");
            if(steps.size()<2||steps.size()>50)
                throw new IllegalArgumentException("Pipeline requires 2 to 50 steps");
            int sourceCount=0,sinkCount=0;
            for(int i=0;i<steps.size();i++){
                JsonNode step=steps.get(i);
                if(!step.isObject()||!step.path("type").isTextual())
                    throw new IllegalArgumentException("Each step must define a string type");
                String type=step.path("type").asText();
                if(SOURCES.contains(type)){
                    sourceCount++;
                    if(i!=0) throw new IllegalArgumentException("The source step must be first");
                    if(type.equals("read-doris")) requireText(step,"table");
                    else requireText(step,"uri");
                }else if(TRANSFORMS.contains(type)){
                    if(i==0) throw new IllegalArgumentException("A source step must be first");
                    switch(type){
                        case "filter"->{requireText(step,"column");requireText(step,"operator");}
                        case "select"->{if(!step.path("columns").isArray()||step.path("columns").isEmpty())throw new IllegalArgumentException("select requires columns");}
                        case "derive"->{requireText(step,"operation");requireText(step,"source");requireText(step,"output");}
                        case "aggregate"->{if(!step.path("metrics").isArray()||step.path("metrics").isEmpty()||!step.path("groupBy").isArray())throw new IllegalArgumentException("aggregate requires groupBy and metrics arrays");}
                    }
                }else if(SINKS.contains(type)){
                    sinkCount++;
                    if(i!=steps.size()-1)throw new IllegalArgumentException("The write step must be last");
                    if(type.equals("write-doris"))requireText(step,"table");else requireText(step,"uri");
                }else throw new IllegalArgumentException("Unsupported pipeline step type: "+type);
            }
            if(sourceCount!=1||sinkCount!=1)throw new IllegalArgumentException("Pipeline must have exactly one source and one final write step");
        }catch(Exception ex){
            if(ex instanceof ResponseStatusException response)throw response;
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Invalid pipeline definition: "+ex.getMessage(),ex);
        }
    }
    private static void requireText(JsonNode node,String name){
        JsonNode value=node.get(name);
        if(value==null||!value.isTextual()||value.asText().isBlank())
            throw new IllegalArgumentException("Step property '"+name+"' is required");
    }
}
