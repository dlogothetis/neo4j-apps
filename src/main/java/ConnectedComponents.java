import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.kernel.Uniqueness;
import org.neo4j.tooling.GlobalGraphOperations;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.kernel.Traversal;

import java.io.IOException;

public class ConnectedComponents {

  private static final RelationshipType FRIEND = 
      DynamicRelationshipType.withName("FRIEND");

  public static void main(String[] args) throws IOException {
    Options options = new Options();
    Option opt = new Option("f", true, "input edge file");
    opt.setRequired(true);
    options.addOption(opt);
    opt = new Option("d", true, "graph db directory");
    opt.setRequired(true);
    options.addOption(opt);
    opt = new Option("p", false, "print weights");
    opt.setRequired(false);
    options.addOption(opt);
    opt = new Option("t", false, "print total time");
    opt.setRequired(false);
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
    boolean print = cmd.hasOption('p');
    boolean printTime = cmd.hasOption('t');

    GraphDatabaseService graphDb = 
        new GraphDatabaseFactory().newEmbeddedDatabase(dbDirectory);
    
    long time = System.currentTimeMillis();
    int ccCount = getConnectedComponentsCount(graphDb);
    System.out.println("#components: "+ccCount+
        " time: "+(System.currentTimeMillis()-time));
  }

  public static int getConnectedComponentsCount(GraphDatabaseService db) 
      throws IOException {
    int CCid = 0;
    for (Node n : GlobalGraphOperations.at( db ).getAllNodes() ) {
      if(!n.hasProperty("CCId")) {
        Transaction tx = db.beginTx();
        try {
          Traverser traverser = Traversal.description()
              .breadthFirst()
              .relationships(FRIEND, Direction.BOTH)
              .evaluator(Evaluators.excludeStartPosition())
              .uniqueness(Uniqueness.NODE_GLOBAL)
              .traverse(n);
          int currentCCid = CCid;
          CCid++;
          n.setProperty("CCId", currentCCid);
          for ( org.neo4j.graphdb.Path p : traverser ) {
            p.endNode().setProperty("CCId", currentCCid);
          }
          tx.success();
        }
        catch ( Exception e ) {
          tx.failure();
        } finally {
          tx.finish();
        }
      }
    }
    return CCid;
  }
}