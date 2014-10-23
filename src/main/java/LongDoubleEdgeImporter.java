import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;


/**
 * A simple batch importer for neo4j. Imports a weighted graph in edge format
 * with a fixed schema.
 * 
 * Usage: java Importer <graph file> <db directory>
 * 
 * @author dl
 *
 */
public class LongDoubleEdgeImporter {

  private static final String SPLITTER = "\\s+";

  public static void main(String args[]) throws FileNotFoundException {
    Options options = new Options();
    Option opt = new Option("f", true, "input graph file");
    opt.setRequired(true);
    options.addOption(opt);
    opt = new Option("d", true, "graph db directory");
    opt.setRequired(true);
    options.addOption(opt);
    
    CommandLineParser parser = new BasicParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println("Problem parsing command");
      System.out.println(e);
      System.exit(-1);
    }
    
    String graphFile = cmd.getOptionValue("f");
    String dbDirectory = cmd.getOptionValue("d");
    
    Long2ObjectOpenHashMap<Long> vertex2node = new Long2ObjectOpenHashMap<Long>();
    Scanner fileScanner = new Scanner(new File(graphFile));

    BatchInserter inserter = BatchInserters.inserter(dbDirectory);
    final Label label = DynamicLabel.label("Node");
    final RelationshipType friend = DynamicRelationshipType.withName("FRIEND");
    Map<String, Object> nodeProperties = new HashMap<String, Object>();
    Map<String, Object> edgeProperties = new HashMap<String, Object>();
    
    while (fileScanner.hasNextLine()) {
      String line = fileScanner.nextLine();
      String[] tokens = line.split(SPLITTER);
  
      if (tokens.length!=3) {
        System.out.println("Incorrect line:"+line);
        System.exit(0);
      }
  
      long srcId = Long.parseLong(tokens[0]);
      long dstId = Long.parseLong(tokens[1]);
      double weight = Double.parseDouble(tokens[2]);

      Long srcNode = vertex2node.get(srcId);
      if (srcNode==null) {
        nodeProperties.put("id", srcId);
        srcNode = inserter.createNode(nodeProperties, label);  
        vertex2node.put(srcId, new Long(srcNode));
      } 

      Long dstNode = vertex2node.get(dstId);
      if (dstNode==null) {
        nodeProperties.put("id", dstId);
        dstNode = inserter.createNode(nodeProperties, label);  
        vertex2node.put(dstId, new Long(dstNode));
      }

      edgeProperties.put("weight", weight);
      inserter.createRelationship(srcNode, dstNode, friend, edgeProperties);
    }
  
    inserter.shutdown();
    fileScanner.close();
  }
}
