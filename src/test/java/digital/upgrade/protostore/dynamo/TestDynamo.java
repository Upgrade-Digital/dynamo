package digital.upgrade.protostore.dynamo;

import net.sitemorph.protostore.CrudException;
import net.sitemorph.protostore.CrudIterator;
import net.sitemorph.protostore.MessageNotFoundException;
import digital.upgrade.protostore.dynamo.TestModel.Fate;
import digital.upgrade.protostore.dynamo.TestModel.TestDynamoMessage;

import com.google.protobuf.ByteString;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Test account for dynamo.
 *
 * @author damien@sitemorph.net
 */
public class TestDynamo {

  private static final String ACCESS_KEY = "ACCESS_KEY",
      SECRET_KEY = "SECRET_KEY";
  private TestDynamoMessage message;

  DynamoUrnFieldStore<TestDynamoMessage> getStore() throws CrudException {
    Properties properties = new Properties();
    try {
      properties.load(new FileReader(new File("aws_credentials.properties")));
    } catch (IOException e) {
      throw new CrudException("Storage configuration error, credentials not " +
          "found in aws_credentials.properties (in resources?)", e);
    }
    return new DynamoUrnFieldStore.Builder<TestDynamoMessage>()
        .setAccessKey(properties.getProperty(ACCESS_KEY))
        .setSecretKey(properties.getProperty(SECRET_KEY))
        .setPrototype(TestDynamoMessage.newBuilder())
        .withSecondaryIndex("secondary")
        .build();
  }

  @Test
  public void testCreate() throws CrudException {
    DynamoUrnFieldStore<TestDynamoMessage> store = getStore();
    message = store.create(TestDynamoMessage.newBuilder()
        .setALong(Long.MAX_VALUE)
        .setAInt(Integer.MIN_VALUE)
        .setABool(true)
        .setAString("Hello World!")
        .setAFate(Fate.TO_BE)
        .setAFloat(3.14F)
        .setADouble(3.1452793)
        .setAByte(ByteString.copyFrom(new byte[]{7}))
        .setSecondary("b"));
  }

  @Test(dependsOnMethods = "testCreate")
  public void testRead() throws CrudException {
    DynamoUrnFieldStore<TestDynamoMessage> store = getStore();
    CrudIterator<TestDynamoMessage> iterator = store.read(
        TestDynamoMessage.newBuilder()
            .setUrn(message.getUrn()));
    assertTrue(iterator.hasNext(), "Iterator should have at least one");
    TestDynamoMessage test = iterator.next();
    assertFalse(iterator.hasNext(), "Iterator shouldn't have another message");
    equalMessage(test, message);
  }

  private void equalMessage(TestDynamoMessage test, TestDynamoMessage expect) {
    assertEquals(test.getUrn(), expect.getUrn(), "urn");
    assertEquals(test.getALong(), expect.getALong(), "long");
    assertEquals(test.getAInt(), expect.getAInt(), "int");
    assertEquals(test.getABool(), expect.getABool(), "bool");
    assertEquals(test.getAString(), expect.getAString(), "string");
    assertEquals(test.getAFate(), expect.getAFate(), "Fate");
    assertEquals(test.getAFloat(), expect.getAFloat(), "float");
    assertEquals(test.getADouble(), expect.getADouble(), "double");
    assertEquals(test.getAByte().toByteArray(), expect.getAByte().toByteArray(), "bytes");
    assertFalse(test.hasChange(), "should not have optional field");
    assertEquals(test.getSecondary(), expect.getSecondary(), "secondary index");
  }

  @Test
  public void testReadNone() throws CrudException {
    DynamoUrnFieldStore<TestDynamoMessage> store = getStore();
    CrudIterator<TestDynamoMessage> messages = store.read(TestDynamoMessage.newBuilder()
        .setUrn(UUID.randomUUID().toString()));
    assertFalse(messages.hasNext(), "Message iterator should be empty");
  }

  @Test(dependsOnMethods = "testCreate")
  public void testUpdate() throws CrudException {
    DynamoUrnFieldStore<TestDynamoMessage> store = getStore();
    TestDynamoMessage updated = store.update(message.toBuilder()
        .setChange("Judas"));
  }



  @Test(dependsOnMethods = "testCreate", expectedExceptions = MessageNotFoundException.class)
  public void testUpdateNone() throws CrudException {
    DynamoUrnFieldStore<TestDynamoMessage> store = getStore();
    store.update(message.toBuilder()
        .setUrn(UUID.randomUUID().toString()));
  }
  
  @Test(dependsOnMethods = "testUpdate")
  public void testDeleteOne() throws CrudException {
    DynamoUrnFieldStore<TestDynamoMessage> store = getStore();
    store.delete(message);
  }

  @Test(dependsOnMethods = "testCreate", expectedExceptions = MessageNotFoundException.class)
  public void testDeleteNone() throws CrudException {
    DynamoUrnFieldStore<TestDynamoMessage> store = getStore();
    store.delete(message.toBuilder()
        .setUrn(UUID.randomUUID().toString())
        .build());
  }
  
  @Test(dependsOnMethods = "testCreate")
  public void testSecondaryCreate() throws CrudException {
    DynamoUrnFieldStore<TestDynamoMessage> store = getStore();
    TestDynamoMessage second = store.create(message.toBuilder()
        .setSecondary("a"));
    CrudIterator<TestDynamoMessage> read = store.read(TestDynamoMessage.newBuilder()
        .setUrn(second.getUrn()));
    assertTrue(read.hasNext(), "Expected read to succeed");
    equalMessage(read.next(), second);
    store.create(message.toBuilder()
        .setSecondary("a"));
    store.create(message.toBuilder()
        .setSecondary("c"));
  }
  
  @Test(dependsOnMethods = "testSecondaryCreate")
  public void testSecondaryIndex() throws CrudException {
    DynamoUrnFieldStore<TestDynamoMessage> store = getStore();
    CrudIterator<TestDynamoMessage> read = store.read(TestDynamoMessage.newBuilder()
        .setSecondary("a"));
    assertTrue(read.hasNext(), "Expected a message");
    assertEquals(read.next().getSecondary(), "a", "Expected secondary index");
    assertTrue(read.hasNext(), "Expected a message");
    assertEquals(read.next().getSecondary(), "a", "Expected secondary index");
    assertFalse(read.hasNext(), "Only expected two message " + read.next());
  }

  // TODO Test whether multiple pages are returned for the iterator from item
  // collections#

  @AfterClass
  @Test(alwaysRun = true)
  public void testReadAll() throws CrudException {
    DynamoUrnFieldStore<TestDynamoMessage> store = getStore();
    CrudIterator<TestDynamoMessage> read = store.read(
        TestDynamoMessage.newBuilder());
    int deleted = 0;
    while (read.hasNext()) {
      deleted++;
      store.delete(read.next());
    }
    read.close();
    read = store.read(TestDynamoMessage.newBuilder());
    assertFalse(read.hasNext(), "All messages should be deleted");
    assertEquals(deleted, 2, "Expected 2 deleted");
  }


}
