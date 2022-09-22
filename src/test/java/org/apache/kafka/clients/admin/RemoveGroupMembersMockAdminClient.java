package org.apache.kafka.clients.admin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;
import org.apache.kafka.common.protocol.Errors;

public class RemoveGroupMembersMockAdminClient extends MockAdminClient {

  Map<String, MemberIdentity> instances = new HashMap<>();

  public RemoveGroupMembersMockAdminClient(Map<String, MemberIdentity> instances) {
    super();
    this.instances = instances;
  }

  public RemoveGroupMembersMockAdminClient(List<Node> brokers, Node controller) {
    super(brokers, controller);
  }

  @Override
  public synchronized RemoveMembersFromConsumerGroupResult removeMembersFromConsumerGroup(
      String groupId, RemoveMembersFromConsumerGroupOptions options) {
    Map<MemberIdentity, Errors> res = new HashMap<>();

    options.members().stream()
        .forEach(
            memberToRemove -> {
              if (instances.containsKey(memberToRemove.groupInstanceId())) {
                res.put(instances.get(memberToRemove.groupInstanceId()), Errors.NONE);
              } else {
                res.put(instances.get(memberToRemove.groupInstanceId()), Errors.UNKNOWN_MEMBER_ID);
              }
            });

    KafkaFutureImpl<Map<MemberIdentity, Errors>> future =
        new KafkaFutureImpl<Map<MemberIdentity, Errors>>();
    future.complete(res);

    return new RemoveMembersFromConsumerGroupResult(future, options.members());
  }

  @Override
  public synchronized DescribeConsumerGroupsResult describeConsumerGroups(
      Collection<String> groupIds, DescribeConsumerGroupsOptions options) {
    Map<String, KafkaFuture<ConsumerGroupDescription>> res = new HashMap<>();

    for (String groupId : groupIds) {
      KafkaFutureImpl<ConsumerGroupDescription> consumerGroupDescription = new KafkaFutureImpl<>();

      Collection<MemberDescription> members = new ArrayList<>();
      instances.values().stream()
          .forEach(
              memberIdentity -> {
                members.add(
                    new MemberDescription(
                        memberIdentity.memberId(),
                        Optional.of(memberIdentity.groupInstanceId()),
                        "clientId",
                        "host",
                        new MemberAssignment(Collections.emptySet())));
              });

      consumerGroupDescription.complete(
          new ConsumerGroupDescription(
              groupId, true, members, "partitionAssignor", ConsumerGroupState.STABLE, null));
      res.put(groupId, consumerGroupDescription);
    }

    return new DescribeConsumerGroupsResult(res);
  }
}
