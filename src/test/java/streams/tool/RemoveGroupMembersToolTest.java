package streams.tool;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.RemoveGroupMembersMockAdminClient;
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class RemoveGroupMembersToolTest {

  final static Map<String, MemberIdentity> instances = new HashMap<>();

  @BeforeAll
  public static void setup() {
    instances.put("instanceId", new MemberIdentity().setMemberId("").setGroupInstanceId("instanceId"));
  }

  @Test
  public void ok() {
    try (final RemoveGroupMembersMockAdminClient adminClient = new RemoveGroupMembersMockAdminClient(instances)) {

      RemoveGroupMembersTool removeGroupMembersTool = new RemoveGroupMembersTool();
      Throwable thrown = catchThrowable(
          () -> removeGroupMembersTool.deleteActiveConsumers("groupId", adminClient, Collections.singletonList("instanceId")));
      assertThat(thrown).doesNotThrowAnyException();
    }
  }

  @Test
  public void unknownMember() {
    try (final RemoveGroupMembersMockAdminClient adminClient = new RemoveGroupMembersMockAdminClient(instances)) {

      RemoveGroupMembersTool removeGroupMembersTool = new RemoveGroupMembersTool();
      Throwable thrown = catchThrowable(
          () -> removeGroupMembersTool.deleteActiveConsumers("groupId", adminClient, Collections.singletonList("unknownInstanceId")));
      assertThat(thrown).isInstanceOf(IllegalStateException.class);
      assertThat(thrown).hasMessageContaining("Refuse to remove following members: [unknownInstanceId] from application groupId because they are no active. Active members: [instanceId]");
    }
  }
}
