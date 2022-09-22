package streams.tool;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.RemoveGroupMembersMockAdminClient;
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;
import org.apache.kafka.common.utils.Exit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RemoveGroupMembersToolTest {

  static final Map<String, MemberIdentity> instances = new HashMap<>();

  @BeforeAll
  public static void setup() {
    instances.put(
        "instanceId", new MemberIdentity().setMemberId("").setGroupInstanceId("instanceId"));
  }

  @BeforeEach
  public void fix() {
    Exit.resetExitProcedure();
  }

  @Test
  public void ok() {
    try (final RemoveGroupMembersMockAdminClient adminClient =
        new RemoveGroupMembersMockAdminClient(instances)) {

      RemoveGroupMembersTool removeGroupMembersTool = new RemoveGroupMembersTool();
      Throwable thrown =
          catchThrowable(
              () ->
                  removeGroupMembersTool.deleteActiveConsumers(
                      "groupId", adminClient, Collections.singletonList("instanceId"), true));
      assertThat(thrown).doesNotThrowAnyException();
    }
  }

  @Test
  public void unknownMember() {
    try (final RemoveGroupMembersMockAdminClient adminClient =
        new RemoveGroupMembersMockAdminClient(instances)) {

      RemoveGroupMembersTool removeGroupMembersTool = new RemoveGroupMembersTool();
      Throwable thrown =
          catchThrowable(
              () ->
                  removeGroupMembersTool.deleteActiveConsumers(
                      "groupId",
                      adminClient,
                      Collections.singletonList("unknownInstanceId"),
                      true));
      assertThat(thrown).isInstanceOf(IllegalStateException.class);
      assertThat(thrown)
          .hasMessageContaining(
              "Refuse to remove following members: [unknownInstanceId] from application \"groupId\" because they are no active. Active members: [instanceId]");
    }
  }

  @Test
  public void prefix() {
    try (final RemoveGroupMembersMockAdminClient adminClient =
        new RemoveGroupMembersMockAdminClient(instances)) {

      RemoveGroupMembersTool removeGroupMembersTool = new RemoveGroupMembersTool();
      Throwable thrown =
          catchThrowable(
              () ->
                  removeGroupMembersTool.deleteAllButPrefix(
                      "groupId", adminClient, "prefix", true));
      assertThat(thrown).doesNotThrowAnyException();
    }
  }

  @Test
  public void allMatchPrefix() {
    try (final RemoveGroupMembersMockAdminClient adminClient =
        new RemoveGroupMembersMockAdminClient(instances)) {

      RemoveGroupMembersTool removeGroupMembersTool = new RemoveGroupMembersTool();
      Throwable thrown =
          catchThrowable(
              () ->
                  removeGroupMembersTool.deleteAllButPrefix(
                      "groupId", adminClient, "instanceId", true));
      assertThat(thrown).isInstanceOf(IllegalStateException.class);
      assertThat(thrown)
          .hasMessageContaining(
              "Refuse to remove members that doesn't fit prefix \"instanceId\" from application \"groupId\" "
                  + "because none found");
    }
  }
}
