package digital.upgrade.dynamo;

import net.sitemorph.protostore.CrudException;
import net.sitemorph.protostore.CrudIterator;
import net.sitemorph.protostore.CrudStore;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.GetItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

import java.util.UUID;

/**
 * Dynamo store for protobuf messages.
 *
 * @author damien@upgrade-digital.com
 */

public class DynamoUrnCrudStore <T extends Message> implements CrudStore<T> {

  private T.Builder prototype;
  private FieldDescriptor urnField;
  private DynamoDB database;
  private String tableName;
  private String urnFieldName;
  private Table table;

  private DynamoUrnCrudStore() {

  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public T create(Message.Builder builder) throws CrudException {
    try {
      Item item = new Item();
      for (FieldDescriptor fieldDescriptor : builder.getDescriptorForType().getFields()) {
        if (fieldDescriptor.equals(urnField)) {
          builder.setField(urnField, UUID.randomUUID().toString());
          item.withPrimaryKey(fieldDescriptor.getName(), builder.getField(fieldDescriptor));

        } else {
          setValue(fieldDescriptor, builder, item);
        }
      }
      table.putItem(item);
      return (T)builder.build();
    } catch (RuntimeException e) {
      throw new CrudException("Error creating message", e);
    }

  }

  @Override
  public CrudIterator<T> read(Message.Builder builder) throws CrudException {
    if (builder.hasField(urnField)) {
      return readUrn(builder);
    }
    // if has a hash key value use
    // table.query()
    throw new CrudException("Not implemented");
  }

  private CrudIterator<T> readUrn(Message.Builder builder) {
    GetItemOutcome outcome = table.getItemOutcome(urnField.getName(), builder.getField(urnField));
    return new GetOutcomeIterator<T>(outcome, builder);
  }

  @Override
  public T update(Message.Builder builder) throws CrudException {
    throw new CrudException("Not implemented");
  }

  @Override
  public void delete(T message) throws CrudException {
    DeleteItemOutcome outcome = table.deleteItem(urnField.getName(),
        message.getField(urnField));
    if (null == outcome.getItem()) {
      throw new CrudException("Message was not deleted: " + message.getField(urnField));
    }
  }

  @Override
  public void close() throws CrudException {
  }

  Item setValue(FieldDescriptor field, Message.Builder builder, Item item) {
    if (!builder.hasField(field)) {
      return item.withNull(field.getName());
    }
    switch (field.getType()) {
      case INT32:
      case SINT32:
      case UINT32:
        item.withInt(field.getName(), (Integer)builder.getField(field));
        break;
      case INT64:
      case UINT64:
      case SINT64:
        item.withLong(field.getName(), (Long)builder.getField(field));
        break;
      case STRING:
        item.withString(field.getName(), (String)builder.getField(field));
        break;
      case BYTES:
        item.withBinary(field.getName(), (byte[])builder.getField(field));
        break;
      case BOOL:
        item.withBoolean(field.getName(), (Boolean)builder.getField(field));
        break;
      case FLOAT:
        item.withFloat(field.getName(), (Float)builder.getField(field));
        break;
      case DOUBLE:
        item.withDouble(field.getName(), (Double)builder.getField(field));
        break;
      case ENUM:
        item.withString(field.getName(), ((Descriptors.EnumValueDescriptor)builder.getField(field)).getName());
        break;
      case MESSAGE:
      case GROUP:
      default:
        throw new IllegalArgumentException("Unsupported field type in message: " + field.getType().name());
    }
    return item;
  }

  public static class Builder <M extends Message> {

    private DynamoUrnCrudStore<M> store = new DynamoUrnCrudStore<M>();

    private ProfileCredentialsProvider credentials;

    private Builder() {}

    public Builder setCredentials(ProfileCredentialsProvider credentials) {
      this.credentials = credentials;
      return this;
    }

    public Builder setTableName(String tableName) {
      store.tableName = tableName;
      return this;
    }

    public Builder setPrototype(Message.Builder prototype) {
      store.prototype = prototype;
      return this;
    }

    public Builder setUrnField(String urnField) {
      store.urnFieldName = urnField;
      if (null == store.prototype) {
        throw new IllegalStateException("Prototype not set so can't look up fields");
      }
      for (FieldDescriptor field : store.prototype.getDescriptorForType().getFields()) {
        store.urnField = field;
      }
      if (null == store.urnField) {
        throw new IllegalArgumentException("Could not find urn field in field list");
      }
      return this;
    }

    public DynamoUrnCrudStore<M> build() {
      if (null == credentials) {
        throw new IllegalStateException("Can't build dynamo store as no credentials set");
      }
      store.database = new DynamoDB(new AmazonDynamoDBClient(credentials));
      store.table = store.database.getTable(store.tableName);
      return store;
    }
  }
}
