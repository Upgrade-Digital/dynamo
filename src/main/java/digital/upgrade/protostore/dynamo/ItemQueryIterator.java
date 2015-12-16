package digital.upgrade.protostore.dynamo;

import net.sitemorph.protostore.CrudException;
import net.sitemorph.protostore.CrudIterator;

import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

import java.util.Iterator;

/**
 * Dynamo database scan iterator which uses a secondary index to scan for a
 * collection of results.
 *
 * @author damien@sitemorph.net
 */
public class ItemQueryIterator<T extends Message> implements CrudIterator<T> {

  private final T.Builder prototype;
  private final FieldDescriptor index;
  private final Table table;
  private ItemCollection<QueryOutcome> outcome;
  private Iterator<Item> itemIterator;

  public ItemQueryIterator(T.Builder prototype, FieldDescriptor index,
      Table table) {
    this.prototype = prototype;
    this.index = index;
    this.table = table;
  }

  @Override
  public T next() throws CrudException {
    T.Builder builder = prototype.clone();
    Item item = itemIterator.next();
    ItemArrayIterator.setFields(item, builder);
    return (T)builder.build();
  }

  @Override
  public boolean hasNext() throws CrudException {
    return itemIterator.hasNext();
  }

  @Override
  public void close() throws CrudException {
  }

  public void initialise() {
    Index global = table.getIndex(index.getName() + "-index");
    KeyAttribute key = new KeyAttribute(index.getName(), prototype.getField(index));
    QuerySpec querySpec = new QuerySpec();
    querySpec.withHashKey(key);
    outcome = global.query(querySpec);
    itemIterator = outcome.iterator();
  }
}
