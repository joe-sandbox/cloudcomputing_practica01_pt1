package mx.iteso.desi.cloud.lp1;

import java.net.UnknownHostException;
import java.util.Set;
import java.util.Iterator;
import java.util.HashSet;
import mx.iteso.desi.cloud.keyvalue.KeyValueStoreFactory;
import mx.iteso.desi.cloud.keyvalue.IKeyValueStorage;
import mx.iteso.desi.cloud.keyvalue.PorterStemmer;

public class QueryImages {
  IKeyValueStorage imageStore;
  IKeyValueStorage titleStore;
	
  public QueryImages(IKeyValueStorage imageStore, IKeyValueStorage titleStore) 
  {
	  this.imageStore = imageStore;
	  this.titleStore = titleStore;
  }
	
  public Set<String> query(String word)
  {
    // TODO: Return the set of URLs that match the given word,
    //       or an empty set if there are no matches
      Set<String> set = this.titleStore.get(word);
      if(set==null)set = new HashSet<>();
      return set;
  }
        
  public void close()
  {
    if(this.imageStore!=null)
      this.imageStore.close();
    if(this.titleStore!=null)
      this.titleStore.close();
  }
	
  public static void main(String args[]) 
  {
    // TODO: Add your own name here
    System.out.println("*** Alumno: _____________________ (Exp: _________ )");

    // TODO: get KeyValueStores
    IKeyValueStorage imageStore = null;
    IKeyValueStorage titleStore = null;
    try {

      imageStore = KeyValueStoreFactory.getNewKeyValueStore(Config.storeType,
              "images");
      imageStore.sync();
      titleStore = KeyValueStoreFactory.getNewKeyValueStore(Config.storeType,
              "terms");
      titleStore.sync();
      QueryImages myQuery = new QueryImages(imageStore, titleStore);

      for (int i=0; i<args.length; i++) {
        System.out.println(args[i]+":");
        String needle = PorterStemmer.stem(args[i]);
        Set<String> result = myQuery.query(needle);
        Iterator<String> iter = result.iterator();
        while (iter.hasNext())
          System.out.println("  - "+iter.next());
      }

      myQuery.close();
    } catch (Exception e) {
      e.printStackTrace();
    }

    

  }
}

