package digital.upgrade.dynamo;

import net.sitemorph.protostore.CrudException;
import net.sitemorph.protostore.CrudIterator;

import com.amazonaws.services.dynamodbv2.document.GetItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;

/**
 * Iterate over the get outcome result.
 *
 * @author damien@upgrade-digital.com
 */
public class GetOutcomeIterator<T> implements CrudIterator<T> {
  private final Builder builder;
  private final GetItemOutcome outcome;

  public GetOutcomeIterator(GetItemOutcome outcome, Builder builder) {
    this.builder = builder;
    this.outcome = outcome;
  }

  @Override
  public T next() throws CrudException {
    Message.Builder next = builder.clone();
    Item item = outcome.getItem();
    for (FieldDescriptor field : next.getDescriptorForType().getFields()) {
      // for field get the desired type and set it's proto value.
    }
    return (T)builder.build();
  }

  @Override
  public boolean hasNext() throws CrudException {
    return true;
  }

  @Override
  public void close() throws CrudException {

  }
}
