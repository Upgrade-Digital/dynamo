package digital.upgrade.protostore.dynamo;

import net.sitemorph.protostore.CrudException;
import net.sitemorph.protostore.CrudIterator;
import net.sitemorph.protostore.CrudStore;
import net.sitemorph.protostore.MessageNotFoundException;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.AttributeUpdate;
import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Expected;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Dynamo store that implements
 */
public class DynamoUrnFieldStore<T extends Message> implements CrudStore<T> {

  private static final Logger log = LoggerFactory.getLogger("DynamoFieldStore");

  private DynamoDB dynamo;
  private Table table;

  private FieldDescriptor urnField;
  private T.Builder prototype;
  private Set<FieldDescriptor> secondaryIndexes = Sets.newHashSet();

  @Override
  public T create(T.Builder builder) throws CrudException {
    Item item = new Item();
    String urn = UUID.randomUUID().toString();
    builder.setField(urnField, urn);
    item.withPrimaryKey(urnField.getName(), urn);
    for (FieldDescriptor fieldDescriptor : prototype.getDescriptorForType().getFields()) {
      if (fieldDescriptor.equals(urnField)) {
        continue;
      }
      withField(builder, fieldDescriptor, item);
    }
    PutItemOutcome result = table.putItem(item);
    return (T) builder.build();
  }



  @Override
  public CrudIterator<T> read(T.Builder builder) throws CrudException {
    if (builder.hasField(urnField)) {
      GetItemSpec itemSpec = new GetItemSpec();
      itemSpec.withPrimaryKey(new PrimaryKey(urnField.getName(), builder.getField(urnField)));
      itemSpec.withConsistentRead(true);
      return new ItemArrayIterator<T>(builder, table.getItem(itemSpec));
    }
    for (FieldDescriptor index : secondaryIndexes) {
      if (builder.hasField(index)) {
        ItemQueryIterator<T> iterator = new ItemQueryIterator<T>(builder, index, table);
        iterator.initialise();
        return iterator;
      }
    }
    ItemScanIterator<T> iterator = new ItemScanIterator<T>(builder, table);
    iterator.initialise();
    return iterator;
  }

  @Override
  public T update(T.Builder builder) throws CrudException {
    List<FieldDescriptor> fieldList = prototype.getDescriptorForType().getFields();
    AttributeUpdate[] updates = new AttributeUpdate[fieldList.size() - 1];
    PrimaryKey urn = new PrimaryKey(urnField.getName(), builder.getField(urnField));
    int i = 0;
    for (FieldDescriptor field : fieldList) {
      if (field.equals(urnField)) {
        continue;
      }
      AttributeUpdate update = new AttributeUpdate(field.getName());
        withField(builder, field, update);
      updates[i++] = update;
    }
    Expected exists = new Expected(urnField.getName())
        .exists();
    UpdateItemSpec spec = new UpdateItemSpec()
        .withPrimaryKey(urn)
        .withAttributeUpdate(updates)
        .withExpected(exists);

    try {
      UpdateItemOutcome outcome = table.updateItem(spec);
    } catch (ConditionalCheckFailedException e) {
      throw new MessageNotFoundException("Update failed. Message not found: " +
          builder.getField(urnField));
    }
    // check for ConditionalCheckFailedException on conditional write
    return (T)builder.build();
  }

  @Override
  public void delete(T message) throws CrudException {
    PrimaryKey key = new PrimaryKey(urnField.getName(), (String)message.getField(urnField));
    Expected exists = new Expected(urnField.getName()).exists();
    try {
      DeleteItemOutcome outcome = table.deleteItem(key, exists);
    } catch (ConditionalCheckFailedException e) {
      throw new MessageNotFoundException("Delete failed for unkonwn message: " +
          message.getField(urnField));
    }
  }

  @Override
  public void close() throws CrudException {
    try {
      table.waitForAllActiveOrDelete();
    } catch (InterruptedException e) {
      throw new CrudException("Uncommitted data", e);
    }
  }

  void withField(T.Builder builder, FieldDescriptor field, AttributeUpdate update)
      throws CrudException {
    if (!builder.hasField(field)) {
      update.delete();
      return;
    }
    switch (field.getType()) {
      case INT64:
      case SINT64:
      case SFIXED64:
      case UINT64:
      case FIXED64:
        update.put((Long)builder.getField(field));
        break;

      case SINT32:
      case UINT32:
      case SFIXED32:
      case FIXED32:
      case INT32:
        update.put((Integer) builder.getField(field));
        break;

      case BOOL:
        update.put((Boolean) builder.getField(field));
        break;

      case STRING:
        update.put((String) builder.getField(field));
        break;

      case ENUM:
        //statement.setString(index, ((Enum)value).name());
        update.put(((Descriptors.EnumValueDescriptor) builder.getField(
            field)).getName());
        break;

      case FLOAT:
        update.put((Float) builder.getField(field));
        break;

      case DOUBLE:
        update.put((Double) builder.getField(field));
        break;

      case BYTES :
        update.put(((ByteString) builder.getField(field)).toByteArray());
        break;

      default:
        throw new CrudException("Index could not be generated for " +
            "unsupported type: " + field.getType().name());
    }
  }

  void withField(T.Builder builder, FieldDescriptor fieldDescriptor,
      Item item)
      throws CrudException {
    String name = fieldDescriptor.getName();
    if (!builder.hasField(fieldDescriptor)) {
      item.withNull(name);
      return;
    }
    switch (fieldDescriptor.getType()) {
      case INT64:
      case SINT64:
      case SFIXED64:
      case UINT64:
      case FIXED64:
        item.withLong(name, (Long)builder.getField(fieldDescriptor));
        break;

      case SINT32:
      case UINT32:
      case SFIXED32:
      case FIXED32:
      case INT32:
        item.withInt(name, (Integer)builder.getField(fieldDescriptor));
        break;

      case BOOL:
        item.withBoolean(name, (Boolean)builder.getField(fieldDescriptor));
        break;

      case STRING:
        item.withString(name, (String) builder.getField(fieldDescriptor));
        break;

      case ENUM:
        //statement.setString(index, ((Enum)value).name());
        item.withString(name,
            ((Descriptors.EnumValueDescriptor) builder.getField(
                fieldDescriptor)).getName());
        break;

      case FLOAT:
        item.withFloat(name, (Float) builder.getField(fieldDescriptor));
        break;

      case DOUBLE:
        item.withDouble(name, (Double)builder.getField(fieldDescriptor));
        break;

      case BYTES :
        item.withBinary(name, ((ByteString)builder.getField(fieldDescriptor)).toByteArray());
        break;

      default:
        throw new CrudException("Index could not be generated for " +
            "unsupported type: " + fieldDescriptor.getType().name());
    }
  }

  public static class Builder<M extends Message> {

    private DynamoUrnFieldStore<M> result = new DynamoUrnFieldStore<M>();
    private String accessKey;
    private String secretKey;

    private String urnFieldName = "urn";
    private Region region = Region.getRegion(Regions.EU_WEST_1);

    public Builder<M> setAccessKey(String accessKey) {
      this.accessKey = accessKey;
      return this;
    }

    public Builder<M> setSecretKey(String secretKey) {
      this.secretKey = secretKey;
      return this;
    }

    public Builder<M> setPrototype(M.Builder prototype) {
      result.prototype = prototype;
      return this;
    }

    public Builder<M> withSecondaryIndex(String fieldName)
        throws CrudException {
      for (FieldDescriptor field : result.prototype.getDescriptorForType().getFields()) {
        if (field.getName().equals(fieldName)) {
          result.secondaryIndexes.add(field);
          return this;
        }
      }
      throw new CrudException("Unknown secondary index field name: " + fieldName);
    }

    public DynamoUrnFieldStore<M> build() {
      AmazonDynamoDBClient client = new AmazonDynamoDBClient(
          new AWSCredentialsProvider() {
            @Override
            public AWSCredentials getCredentials() {
              return new BasicAWSCredentials(accessKey, secretKey);
            }

            @Override
            public void refresh() {
            }
          });
      client.setRegion(region);
      DynamoDB dynamo = new DynamoDB(client);
      result.dynamo = dynamo;
      result.table = dynamo.getTable(result.prototype.getDescriptorForType().getName());
      log.info("Created table accessor for {}", result.prototype.getDescriptorForType().getName());

      for (FieldDescriptor field : result.prototype.getDescriptorForType().getFields()) {
        if (field.getName().equals(urnFieldName)) {
          result.urnField = field;
        }
      }

      return (DynamoUrnFieldStore<M>)result;
    }
  }
}
