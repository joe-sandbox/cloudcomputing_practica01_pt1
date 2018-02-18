package mx.iteso.desi.cloud.lp1;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import mx.iteso.desi.cloud.keyvalue.IKeyValueStorage;
import mx.iteso.desi.cloud.keyvalue.KeyValueStoreFactory;
import mx.iteso.desi.cloud.keyvalue.ParseTriples;
import mx.iteso.desi.cloud.keyvalue.PorterStemmer;
import mx.iteso.desi.cloud.keyvalue.Triple;

public class IndexImages {
  ParseTriples parser;
  IKeyValueStorage imageStore, titleStore;
    
  public IndexImages(IKeyValueStorage imageStore, IKeyValueStorage titleStore) {
	  this.imageStore = imageStore;
	  this.titleStore = titleStore;
  }

  private Map<String,String> processImages()throws IOException{
    Map<String,String> images = new HashMap<String,String>();
    ParseTriples imgParser = new ParseTriples(Config.imageFileName);
    Triple triple = imgParser.getNextTriple();
    int globalCounter = 0;
    while(triple != null) {
      //check against filter...
        int lastSlash = triple.getSubject().lastIndexOf("/");
        String subject = triple.getSubject().substring(lastSlash+1);

      if (subject.startsWith(Config.filter)) {
        if (triple.getPredicate().endsWith("depiction")) {
          //we add to the list..
          images.put(triple.getSubject(), triple.getObject());
          imageStore.addToSet(triple.getSubject(), triple.getObject());
        }
      }
      triple = imgParser.getNextTriple();
      globalCounter++;
      if((globalCounter%1000)==0){
          //System.out.println("Processed:"+globalCounter+" image items...");
      }
    }
    //System.out.println("Processed:"+globalCounter+" image items...");
    return images;
  }
  private Map<String,Set<String>> processLabels(Map<String,String> images) throws IOException{
    Map<String,Set<String>> titles = new HashMap<String,Set<String>>();
    ParseTriples lblParser = new ParseTriples(Config.titleFileName);
    Triple triple = lblParser.getNextTriple();
    while(triple != null) {
      //check against filter...
        int lastSlash = triple.getSubject().lastIndexOf("/");
        String subject = triple.getSubject().substring(lastSlash+1);
      if (subject.startsWith(Config.filter)) {

        if (images.containsKey(triple.getSubject())) {

          String lbls = triple.getObject();

          if(lbls.contains(" ")){
            //we need to split
            String[] list = lbls.split(" ");

            for(String s : list){
                String key = PorterStemmer.stem(s);
                if(titles.containsKey(key)){
                    Set<String> set = titles.get(key);
                    set.add(triple.getSubject());
                    titles.put(key,set);
                }else {
                    Set<String> set = new HashSet<>();
                    set.add(triple.getSubject());
                    titles.put(key, set);
                }
            }
          }else{
              String key = PorterStemmer.stem(triple.getObject());
              Set<String> set = new HashSet<>();
              set.add(triple.getSubject());
              titles.put(key,set);
          }
        }
      }
      triple = lblParser.getNextTriple();
    }
      int globalCounter = 0;
    for(Map.Entry<String,Set<String>> entry : titles.entrySet()){
        this.titleStore.addToSet(entry.getKey(),entry.getValue());
        globalCounter++;
        if((globalCounter%1000)==0){
            //System.out.println("Processed:"+globalCounter+" term items...");
        }
    }
    return titles;
  }
  public void run(String imageFileName, String titleFileName) throws IOException
  {
    // TODO: This method should load all images and titles 
    //       into the two key-value stores.
      long start = System.currentTimeMillis();
      Map<String,String> images = this.processImages();
      //System.out.print("done processing images!");
      Map<String,Set<String>> lbls = this.processLabels(images);
      //System.out.print("done processing terms!");
      this.imageStore.sync();
      this.titleStore.sync();

  }
  
  public void close() {
    if(this.imageStore!=null)
      this.imageStore.close();
    if(this.titleStore!=null)
      this.titleStore.close();
  }
  
  public static void main(String args[])
  {
    // TODO: Add your own name here
    System.out.println("*** Alumno: _____________________ (Exp: _________ )");
   /*try {

      IKeyValueStorage imageStore = KeyValueStoreFactory.getNewKeyValueStore(Config.storeType,
    			"images");
      IKeyValueStorage titleStore = KeyValueStoreFactory.getNewKeyValueStore(Config.storeType, 
  			"terms");

      IndexImages indexer = new IndexImages(imageStore, titleStore);
      indexer.run(Config.imageFileName, Config.titleFileName);
      System.out.println("Indexing completed");
      indexer.close();

      
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("Failed to complete the indexing pass -- exiting");
    }*/

    System.out.println(PorterStemmer.stem("Clementina"));
  }
}

