package com.sbase.platform.ingestion;
import com.sbase.platform.IngestionConnector;
import org.springframework.stereotype.Component;
import java.util.*;
@Component
public class DorisIngestionConnector extends JdbcIngestionConnector implements IngestionConnector {
 @Override public String type(){return "doris";}
}
