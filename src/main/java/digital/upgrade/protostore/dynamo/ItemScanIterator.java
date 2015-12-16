package digital.upgrade.protostore.dynamo;

import net.sitemorph.protostore.CrudException;
import net.sitemorph.protostore.CrudIterator;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.google.protobuf.Message;

import java.util.Iterator;

/**
 * Scan over a table (reading all rather than using an index).
 *
 * @author damien@upgrade-digital.com
 */
public class ItemScanIterator<T extends Message> implements CrudIterator<T> {

  private final T.Builder prototype;
  private final Table table;
  private Iterator<Item> iterator;

  public ItemScanIterator(T.Builder builder, Table table) {
    this.prototype = builder;
    this.table = table;
  }

  public void initialise() {
    ScanSpec scan = new ScanSpec();
    ItemCollection<ScanOutcome> outcome = table.scan(scan);
    iterator = outcome.iterator();
  }

  @Override
  public T next() throws CrudException {
    T.Builder builder = prototype.clone();
    Item item = iterator.next();
    ItemArrayIterator.setFields(item, builder);
    return (T)builder.build();
  }

  @Override
  public boolean hasNext() throws CrudException {
    return iterator.hasNext();
  }

  @Override
  public void close() throws CrudException {

  }
}
