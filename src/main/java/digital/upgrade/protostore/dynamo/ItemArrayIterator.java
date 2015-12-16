package digital.upgrade.protostore.dynamo;

import net.sitemorph.protostore.CrudException;
import net.sitemorph.protostore.CrudIterator;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;

/**
 * Iterate over a collection of returned items.
 *
 * @author damien@sitemorph.net
 */
public class ItemArrayIterator<T extends Message> implements CrudIterator<T> {

  private static final Item[] EMPTY = new Item[0];

  private Item[] items;
  private final T.Builder builder;
  private int index;

  public ItemArrayIterator(T.Builder builder, Item... items) {
    this.builder = builder;
    if (1 == items.length && null == items[0]) {
      this.items = EMPTY;
    } else {
      this.items = items;
    }
    this.index = 0;
  }

  @Override
  public T next() throws CrudException {
    if (index >= items.length) {
      throw new CrudException("No more items");
    }
    T.Builder newT = builder.clone();
    Item item = items[index++];
    setFields(item, newT);
    return (T)newT.build();
  }

  static void setFields(Item item, Builder builder) throws CrudException {
    for (FieldDescriptor field : builder.getDescriptorForType().getFields()) {
      String name = field.getName();
      if (item.isNull(name)) {
        builder.clearField(field);
        continue;
      }
      Object value;
      switch (field.getType()) {
        case INT64:
        case SINT64:
        case SFIXED64:
        case UINT64:
        case FIXED64:
          value = (Long)item.getLong(name);
          break;

        case SINT32:
        case UINT32:
        case SFIXED32:
        case FIXED32:
        case INT32:
          value = (Integer)item.getInt(name);
          break;

        case BOOL:
          value = (Boolean)item.getBoolean(name);
          break;

        case STRING:
          value = (String)item.getString(name);
          break;

        case ENUM:
          value = field.getEnumType().findValueByName(item.getString(name));
          break;

        case FLOAT:
          value = (Float)item.getFloat(name);
          break;

        case DOUBLE:
          value = (Double)item.getDouble(name);
          break;

        case BYTES :
          value = ByteString.copyFrom(item.getBinary(name));
          break;

        default:
          throw new CrudException("Index could not be generated for " +
              "unsupported type: " + field.getType().name());
      }
      builder.setField(field, value);
    }
  }

  @Override
  public boolean hasNext() throws CrudException {
    return index < items.length;
  }

  @Override
  public void close() throws CrudException {
    items = null;
  }
}
